import logging
import os
from pathlib import Path

import gspread as gp
import pandas as pd
import yaml
from dotenv import load_dotenv

from my_ingestion.core.text import normalize_string

load_dotenv()
pd.set_option("display.max_columns", None)

logger = logging.getLogger(__name__)


def _load_sheets_config(config_path: Path) -> list[dict]:
    """Le config.yml e retorna os workbooks [{key, env, primary, sheets}].

    A URL de cada workbook vem da env var URL_FINANCE_<CHAVE>; o primeiro workbook
    eh o primario (abas sem sufixo), os demais sufixam o nome da aba com a chave.
    """
    config_path = Path(config_path)
    try:
        with config_path.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file) or {}
    except (OSError, yaml.YAMLError) as e:
        logger.error("Falha ao carregar %s: %s", config_path, e)
        return []

    workbooks = []
    for raw_key, entries in (config.get("sheets") or {}).items():
        key = normalize_string(str(raw_key))
        if not key:
            continue

        sheets = []
        for entry in entries or []:
            name = str(entry.get("name", "")).strip()
            if not name:
                continue
            sheets.append({"name": name, "header_row": int(entry.get("header_row", 0))})

        workbooks.append(
            {
                "key": key,
                "env": f"URL_FINANCE_{key.upper()}",
                "primary": not workbooks,  # o primeiro do config manda nos nomes atuais
                "sheets": sheets,
            }
        )
    return workbooks


def connect(credentials: str) -> gp.Client | None:
    """Conecta ao Google via service account (uma unica vez)."""
    try:
        gc = gp.service_account(filename=credentials)
        logger.info("Conexao com Google estabelecida")
        return gc
    except FileNotFoundError:
        logger.error("Arquivo de credenciais nao encontrado: %s", credentials)
    except Exception:
        logger.exception("Falha ao conectar ao Google")
    return None


def _dedupe_header(cells: list) -> list[str]:
    """Normaliza os nomes de coluna; nomeia vazios e desambigua duplicados."""
    header: list[str] = []
    seen: dict[str, int] = {}
    for i, cell in enumerate(cells):
        name = normalize_string(str(cell))
        if not name:
            name = f"col_{i}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        header.append(name)
    return header


def fetch_raw_sheet(
    workbook, name: str, header_row: int, workbook_key: str
) -> pd.DataFrame:
    """Puxa uma aba como esta (texto cru), usando header_row como cabecalho."""
    values = workbook.worksheet(name).get_all_values()
    if header_row >= len(values):
        logger.warning(
            "Aba '%s': header_row %d fora do intervalo (%d linhas)",
            name,
            header_row,
            len(values),
        )
        return pd.DataFrame()

    header = _dedupe_header(values[header_row])
    df = pd.DataFrame(values[header_row + 1 :], columns=header)
    df = df[~(df == "").all(axis=1)]  # descarta linhas totalmente vazias
    # rastreabilidade (analogo a source_path)
    df["source_sheet"] = name
    df["source_workbook"] = workbook_key
    return df


def run_google_finance_etl(
    output_dir: Path,
    credentials: str,
    config_path: Path,
) -> Path:
    """Ingesta bruta das abas do config para CSVs em output_dir. Retorna output_dir."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    workbooks = _load_sheets_config(config_path)
    if not workbooks:
        logger.warning("Nenhuma planilha em %s", config_path)
        return output_dir

    gc = connect(credentials)
    if not gc:
        return output_dir

    for wb in workbooks:
        key, sheets = wb["key"], wb["sheets"]
        logger.info("Planilha '%s': %d aba(s) configurada(s)", key, len(sheets))

        sheet_url = os.getenv(wb["env"])
        if not sheet_url:
            logger.error("%s nao definida; planilha '%s' ignorada", wb["env"], key)
            continue

        try:
            workbook = gc.open_by_url(sheet_url)
        except Exception:
            # uma planilha inacessivel nao pode derrubar as demais
            logger.exception("Falha ao abrir a planilha '%s'", key)
            continue

        for sheet in sheets:
            name = sheet["name"]
            try:
                df = fetch_raw_sheet(workbook, name, sheet["header_row"], key)
            except gp.exceptions.WorksheetNotFound:
                logger.warning("Aba '%s' nao encontrada em '%s'; pulando", name, key)
                continue
            if df.empty:
                logger.warning("Aba '%s' de '%s' vazia; pulando", name, key)
                continue

            stem = (
                normalize_string(name)
                if wb["primary"]
                else f"{normalize_string(name)}_{key}"
            )
            output_path = output_dir / f"google_{stem}.csv"
            df.to_csv(output_path, index=False)
            logger.info(
                "Ingerido '%s' -> %s (%d linhas, %d colunas)",
                name,
                output_path.name,
                len(df),
                len(df.columns),
            )

    logger.info("Google Finance concluido em %s", output_dir)
    return output_dir
