import csv
import logging
import os
import re
import subprocess
from pathlib import Path

from my_ingestion.core.io import list_files

logger = logging.getLogger(__name__)

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
        r["source_file"] = os.path.abspath(path)

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
        "source_file": os.path.abspath(path),
    }


def run_avenue_dividends_etl(input_dir: Path, output_dir: Path) -> Path:
    """Consolida o total de dividendos e juros de todos os PDFs num unico CSV.

    Diferente de run_avenue_etl, nao filtra por variant: junta current e legacy
    de todas as pessoas em um unico dividends_interest.csv, com o layout gravado
    por linha a partir da subpasta. Uma linha por extrato, so com debit/credit da
    linha "Total Dividends And Interest".
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Espera <input_dir>/<pessoa>/<variant>/arquivo.pdf, como run_avenue_etl,
    # mas aceita qualquer variant (parts[1]) para unificar os dois layouts.
    files = [
        f
        for f in list_files(input_dir)
        if f.suffix.lower() == ".pdf" and len(f.relative_to(input_dir).parts) >= 3
    ]
    logger.info("Avenue dividends: %d PDF(s) em %s", len(files), input_dir)
    if not files:
        logger.warning("Nenhum PDF encontrado em %s", input_dir)

    all_rows = []
    for f in files:
        rel = f.relative_to(input_dir).parts
        row = parse_dividends_total(str(f))
        row["person"] = rel[0]
        row["layout"] = rel[1]
        all_rows.append(row)

    all_rows.sort(key=lambda r: (r["period_end"] or "", r["person"], r["layout"]))

    fieldnames = [
        "period_start",
        "period_end",
        "account_number",
        "debit",
        "credit",
        "person",
        "layout",
        "source_file",
    ]

    output_path = output_dir / "dividends_interest.csv"
    with open(output_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in all_rows:
            w.writerow(r)

    logger.info(
        "Avenue dividends: %d linha(s) de %d arquivo(s) -> %s",
        len(all_rows),
        len(files),
        output_path,
    )
    return output_path


def run_avenue_etl(input_dir: Path, output_dir: Path) -> Path:
    """Consolida os PDFs de holdings de todas as pessoas num unico CSV.

    Como o run_avenue_dividends_etl, nao filtra por variant: junta current e
    legacy de todas as pessoas em um unico <output_dir>/avenue.csv, com o layout
    gravado por linha a partir da subpasta. A logica de parsing e a mesma para os
    dois layouts; a distincao fica so nas colunas "person"/"layout".
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Espera <input_dir>/<pessoa>/<variant>/arquivo.pdf; a pessoa vem da 1a parte
    # do caminho relativo e o layout da 2a, como o b3 preserva a pasta da pessoa.
    files = [
        f
        for f in list_files(input_dir)
        if f.suffix.lower() == ".pdf" and len(f.relative_to(input_dir).parts) >= 3
    ]
    logger.info("Avenue: %d PDF(s) em %s", len(files), input_dir)
    if not files:
        logger.warning("Nenhum PDF encontrado em %s", input_dir)

    all_rows = []
    val_report = []
    for f in files:
        rel = f.relative_to(input_dir).parts
        person, layout = rel[0], rel[1]
        rows, validation = parse_file(str(f))
        for r in rows:
            r["person"] = person
            r["layout"] = layout
        all_rows.extend(rows)
        # soma por asset class para validacao
        sums = {}
        for r in rows:
            ac = r["asset_class"]
            mv = r["market_value"]
            if isinstance(mv, (int, float)):
                sums[ac] = sums.get(ac, 0) + mv
        val_report.append((f"{person}/{f.name}", validation, sums))

    all_rows.sort(
        key=lambda r: (
            r["period_end"] or "",
            r["person"],
            r["layout"],
            r["asset_class"],
            r["description"],
        )
    )

    fieldnames = [
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

    output_path = output_dir / "assets.csv"
    with open(output_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in all_rows:
            w.writerow(r)

    logger.info(
        "Avenue: %d linha(s) de %d arquivo(s) -> %s",
        len(all_rows),
        len(files),
        output_path,
    )

    # --- Validacao (total declarado vs soma das linhas) ---
    issues = 0
    for fname, validation, sums in val_report:
        for ac_label, val_key in [
            ("Equities", "Equities"),
            ("Fixed Income", "Fixed Income"),
        ]:
            stated = validation.get(val_key)
            summed = sums.get(ac_label)
            if stated is not None and summed is not None:
                if abs(stated - summed) > 0.05:
                    logger.warning(
                        "MISMATCH %s [%s]: declarado=%s somado=%s",
                        fname,
                        ac_label,
                        stated,
                        round(summed, 2),
                    )
                    issues += 1
        total_stated = validation.get("Total Priced Portfolio")
        total_summed = sum(v for v in sums.values() if isinstance(v, (int, float)))
        if total_stated is not None:
            if abs(total_stated - total_summed) > 0.05:
                logger.warning(
                    "MISMATCH %s [TOTAL]: declarado=%s somado=%s",
                    fname,
                    total_stated,
                    round(total_summed, 2),
                )
                issues += 1
    logger.info("Avenue: %d divergencia(s) em %d arquivo(s)", issues, len(files))

    return output_path
