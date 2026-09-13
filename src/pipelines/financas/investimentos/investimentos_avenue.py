"""Extratos PDF da Avenue (pdftotext): posições (assets) e dividendos.

Sem extract: os PDFs são colocados manualmente em
raw/investments/avenue/<pessoa>/<layout>/. O transform grava assets.csv e
dividends_interest.csv no bronze; load: files -> raw_avenue.assets|dividends_interest.
Registrado em ``investimentos_etl.py``.
"""

import logging
import re
import subprocess
from pathlib import Path

import pandas as pd

from core import PipelineConfig, list_files, reset_bronze, write_csv

logger = logging.getLogger(__name__)

ASSET_COLS = [
    "period_start",
    "period_end",
    "account_number",
    "asset_class",
    "description",
    "symbol_cusip",
    "account_type",
    "quantity",
    "price",
    "market_value",
    "last_period_market_value",
    "pct_change",
    "est_annual_income",
    "pct_of_total_portfolio",
    "person",
    "layout",
    "source_file",
]
DIVIDEND_COLS = [
    "period_start",
    "period_end",
    "account_number",
    "debit",
    "credit",
    "person",
    "layout",
    "source_file",
]

MONTHS = (
    "January|February|March|April|May|June"
    "|July|August|September|October|November|December"
)
PERIOD_RE = re.compile(
    rf"({MONTHS})\s+(\d{{1,2}}),?\s+(\d{{4}})\s*-\s*({MONTHS})\s+(\d{{1,2}}),?\s+(\d{{4}})"
)

MONTH_NUM = {
    m: i + 1
    for i, m in enumerate(
        [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
    )
}


def get_text(path):
    r = subprocess.run(
        ["pdftotext", "-layout", path, "-"], capture_output=True, text=True
    )
    return r.stdout


def get_period(text):
    m = PERIOD_RE.search(text)
    if not m:
        return None, None
    mo1, d1, y1, mo2, d2, y2 = m.groups()
    start = f"{y1}-{MONTH_NUM[mo1]:02d}-{int(d1):02d}"
    end = f"{y2}-{MONTH_NUM[mo2]:02d}-{int(d2):02d}"
    return start, end


def get_account_number(text):
    m = re.search(r"AVENUE ACCOUNT NUMBER:\s*([0-9A-Za-z\-]+)", text)
    if m:
        return m.group(1)
    m = re.search(r"ACCOUNT NUMBER\s+([0-9A-Za-z\-]+(?:\s+RR\s+\w+)?)", text)
    if m:
        return m.group(1).strip()
    return ""


def is_numeric_like(tok):
    return (
        bool(re.match(r"^\$?-?[\d,]+\.?\d*%?$", tok))
        or tok.upper() == "N/A"
        or bool(re.match(r"^<-?\d+\.?\d*%?$", tok))
    )


def split_row(line):
    line = re.sub(r"\$\s+", "$", line)
    parts = re.split(r"\s{2,}", line.strip())
    out = [parts[0]] if parts else []
    for p in parts[1:]:
        subtoks = p.split(" ")
        if len(subtoks) > 1 and all(is_numeric_like(s) for s in subtoks):
            out.extend(subtoks)
        else:
            out.append(p)
    return out


def clean_num(s):
    if s is None:
        return ""
    s = s.strip()
    if s == "" or s.upper() == "N/A":
        return ""
    neg = False
    if s.startswith("-"):
        neg = True
        s = s[1:]
    s = s.replace("$", "").replace(",", "").replace("<", "")
    s = s.rstrip("%")
    try:
        val = float(s)
    except ValueError:
        return ""
    if neg:
        val = -val
    return val


def parse_holding_tokens(parts):
    # parts: [desc, symbol, type, qty, price, mval, ...remaining..., pcttotal]
    desc, symbol, atype, qty, price, mval = (
        parts[0],
        parts[1],
        parts[2],
        parts[3],
        parts[4],
        parts[5],
    )
    pcttotal = parts[-1]
    remaining = parts[6:-1]
    last_period = pct_change = income = ""
    if len(remaining) == 0:
        pass
    elif remaining[0].upper() == "N/A":
        pct_change = "N/A"
        if len(remaining) >= 2:
            income = remaining[1]
    else:
        last_period = remaining[0]
        if len(remaining) == 2:
            pct_change = remaining[1]
        elif len(remaining) >= 3:
            pct_change = remaining[1]
            income = remaining[2]
    return {
        "description": desc.strip(),
        "symbol_cusip": symbol.strip(),
        "account_type": atype.strip(),
        "quantity": clean_num(qty),
        "price": clean_num(price),
        "market_value": clean_num(mval),
        "last_period_market_value": clean_num(last_period),
        "pct_change": pct_change if pct_change == "N/A" else clean_num(pct_change),
        "est_annual_income": clean_num(income),
        "pct_of_total_portfolio": clean_num(pcttotal),
    }


def norm(s):
    return re.sub(r"\s+", "", s).upper()


def parse_file(path):
    text = get_text(path)
    period_start, period_end = get_period(text)
    account_number = get_account_number(text)
    lines = text.split("\n")

    rows = []
    asset_class = None
    in_table = False
    validation = {
        "Equities": None,
        "Fixed Income": None,
        "Total Priced Portfolio": None,
    }

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        n = norm(stripped)

        if "DESCRIPTION" in n and "CUSIP" in n and "TYPE" in n:
            in_table = True
            continue

        if not in_table:
            continue

        if "EQUITIES" in n and "OPTIONS" in n:
            asset_class = "Equities"
            continue
        if n.startswith("FIXEDINCOME"):
            asset_class = "Fixed Income"
            continue
        if n.startswith("TOTALPRICEDPORTFOLIO"):
            parts = split_row(stripped)
            if len(parts) >= 2:
                validation["Total Priced Portfolio"] = clean_num(parts[1])
            break  # end of table
        if stripped.startswith("Total Cash"):
            parts = split_row(stripped)
            # Total Cash (Net Portfolio Balance)   $X   pct%
            mval = clean_num(parts[1]) if len(parts) >= 2 else ""
            pct = clean_num(parts[-1]) if len(parts) >= 1 else ""
            rows.append(
                {
                    "description": "Cash (Net Portfolio Balance)",
                    "symbol_cusip": "CASH",
                    "account_type": "",
                    "quantity": "",
                    "price": "",
                    "market_value": mval,
                    "last_period_market_value": "",
                    "pct_change": "",
                    "est_annual_income": "",
                    "pct_of_total_portfolio": pct,
                    "asset_class": "Cash",
                }
            )
            continue
        if stripped.startswith("Total ") or stripped.upper().startswith("TOTAL "):
            # section subtotal line
            # (Total Equities / Total Fixed Income / TOTAL TREASURIES etc.)
            parts = split_row(stripped)
            label = parts[0].strip()
            mval_tok = next((p for p in parts[1:] if "." in p), None)
            if label.lower() in ("total equities",):
                validation["Equities"] = clean_num(mval_tok) if mval_tok else None
            elif label.lower() in ("total fixed income",):
                validation["Fixed Income"] = clean_num(mval_tok) if mval_tok else None
            continue

        # candidate holding row
        parts = split_row(stripped)
        if (
            len(parts) >= 4
            and re.match(r"^[A-Z]$", parts[2])
            and is_numeric_like(parts[3])
        ):
            h = parse_holding_tokens(parts)
            h["asset_class"] = asset_class or ""
            rows.append(h)
        # else: continuation / subheader / disclosure text -> skip

    for r in rows:
        r["period_start"] = period_start
        r["period_end"] = period_end
        r["account_number"] = account_number
        r["source_file"] = str(Path(path).resolve())

    return rows, validation


def parse_dividends_total(path):
    """Extrai apenas a linha "Total Dividends And Interest" de um extrato.

    Vale para os dois layouts (current/legacy): a linha traz dois numeros lado a
    lado, o primeiro na coluna DEBIT (imposto retido) e o segundo na coluna
    CREDIT (dividendo bruto), posicionais e sem sinal. Guarda ambos positivos,
    como aparecem no PDF. Se a secao nao existir, retorna 0.0/0.0 e loga aviso.
    """
    text = get_text(path)
    period_start, period_end = get_period(text)
    account_number = get_account_number(text)
    lines = text.split("\n")

    debit = 0.0
    credit = 0.0
    found = False
    debit_col = credit_col = None

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        n = norm(stripped)

        # Cabecalho da tabela de atividade: guarda a posicao das colunas
        # DEBIT/CREDIT para o fallback posicional (caso raro de 1 valor so).
        if "DEBIT" in n and "CREDIT" in n and "DESCRIPTION" in n:
            up = line.upper()
            debit_col = up.find("DEBIT")
            credit_col = up.find("CREDIT")
            continue

        if n.startswith("TOTALDIVIDENDSANDINTEREST"):
            found = True
            parts = split_row(stripped)
            nums = [
                clean_num(p)
                for p in parts[1:]
                if is_numeric_like(p) and isinstance(clean_num(p), (int, float))
            ]
            if len(nums) >= 2:
                debit, credit = nums[-2], nums[-1]
            elif len(nums) == 1 and debit_col is not None and credit_col is not None:
                # Um valor so: atribui a coluna mais proxima pela posicao no PDF.
                m = re.search(r"\$?\s*-?[\d,]+\.?\d*", line)
                pos = m.start() if m else 0
                if abs(pos - debit_col) <= abs(pos - credit_col):
                    debit = nums[0]
                else:
                    credit = nums[0]
            elif len(nums) == 1:
                # Sem cabecalho de referencia: assume DEBIT (posicao a esquerda).
                debit = nums[0]
            break

    if not found:
        logger.warning("Sem secao Dividends And Interest em %s", path)

    return {
        "period_start": period_start,
        "period_end": period_end,
        "account_number": account_number,
        "debit": debit,
        "credit": credit,
        "source_file": str(Path(path).resolve()),
    }


def _pdfs(input_dir: Path) -> list[Path]:
    """``<input_dir>/<pessoa>/<layout>/*.pdf`` (pessoa e layout viram colunas)."""
    return [
        f
        for f in list_files(input_dir)
        if f.suffix.lower() == ".pdf" and len(f.relative_to(input_dir).parts) >= 3
    ]


def _validate(fname: str, validation: dict, rows: list[dict]) -> int:
    """Compara os totais declarados no extrato com a soma das linhas (divergências)."""
    sums: dict[str, float] = {}
    for r in rows:
        if isinstance(r["market_value"], int | float):
            sums[r["asset_class"]] = sums.get(r["asset_class"], 0) + r["market_value"]
    issues = 0
    for label in ("Equities", "Fixed Income"):
        stated, summed = validation.get(label), sums.get(label)
        if stated is not None and summed is not None and abs(stated - summed) > 0.05:
            logger.warning(
                f"MISMATCH {fname} [{label}]: declarado={stated} somado={summed:.2f}"
            )
            issues += 1
    total_stated = validation.get("Total Priced Portfolio")
    total_summed = sum(sums.values())
    if total_stated is not None and abs(total_stated - total_summed) > 0.05:
        logger.warning(
            f"MISMATCH {fname} [TOTAL]: declarado={total_stated} "
            f"somado={total_summed:.2f}"
        )
        issues += 1
    return issues


def transform(cfg: PipelineConfig) -> None:
    reset_bronze(cfg)
    files = _pdfs(cfg.landing_dir)
    logger.info(f"Avenue: {len(files)} PDF(s) em {cfg.landing_dir}")

    assets, dividends, issues = [], [], 0
    for f in files:
        person, layout = f.relative_to(cfg.landing_dir).parts[:2]
        rows, validation = parse_file(str(f))
        for r in rows:
            r.update(person=person, layout=layout)
        assets.extend(rows)
        issues += _validate(f"{person}/{f.name}", validation, rows)
        dividends.append(
            {**parse_dividends_total(str(f)), "person": person, "layout": layout}
        )
    logger.info(f"Avenue: {issues} divergencia(s) em {len(files)} arquivo(s)")

    assets_df = pd.DataFrame(assets, columns=ASSET_COLS).sort_values(
        ["period_end", "person", "layout", "asset_class", "description"],
        na_position="first",
    )
    dividends_df = pd.DataFrame(dividends, columns=DIVIDEND_COLS).sort_values(
        ["period_end", "person", "layout"], na_position="first"
    )
    write_csv(assets_df, cfg.bronze_dir, "assets.csv", sep=cfg.bronze_sep)
    write_csv(
        dividends_df, cfg.bronze_dir, "dividends_interest.csv", sep=cfg.bronze_sep
    )
