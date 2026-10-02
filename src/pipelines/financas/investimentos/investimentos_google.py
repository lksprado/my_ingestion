"""Abas do Google Sheets (service account) -> landing JSON bruto -> bronze CSV.

extract: ``get_all_values()`` de cada aba de ``options.sheets`` gravado como JSON
(``{workbook}_{sheet}.json``). transform: aplica ``header_row``, normaliza o
cabeçalho e grava ``<aba>.csv`` (ou ``<aba>_<workbook>.csv`` fora da planilha
primária). load: files -> ``raw_google_sheets.<aba>``. Registrado em
``investimentos_etl.py``.
"""

import json
import logging
from pathlib import Path

import gspread as gp
import pandas as pd
from pydantic import field_validator

from core import (
    PipelineConfig,
    ensure_some_success,
    normalize_string,
    reset_bronze,
    write_csv,
)
from settings import SourceSettings

logger = logging.getLogger(__name__)


class GoogleSheetsSettings(SourceSettings):
    """Service account e URLs das planilhas, uma por chave de ``options.sheets``.

    ``URL_FINANCE__<CHAVE>=...`` vira ``url_finance["<chave>"]``. Sem prefixo: os
    nomes no .env já são esses.
    """

    google_credentials_file: Path
    url_finance: dict[str, str] = {}

    @field_validator("google_credentials_file")
    @classmethod
    def _arquivo_existe(cls, v: Path) -> Path:
        v = v.expanduser()
        if not v.is_file():
            raise ValueError(f"service account não encontrada: {v}")
        return v


def _workbooks(cfg: PipelineConfig) -> list[dict]:
    """[{key, primary, sheets: [{name, header_row, stem}]}] de options.sheets."""
    result = []
    for raw_key, entries in (cfg.options.get("sheets") or {}).items():
        key = normalize_string(str(raw_key))
        primary = not result
        sheets = []
        for e in entries or []:
            name = str(e.get("name", "")).strip()
            if not name:
                continue
            stem = (
                normalize_string(name) if primary else f"{normalize_string(name)}_{key}"
            )
            sheets.append(
                {"name": name, "header_row": int(e.get("header_row", 0)), "stem": stem}
            )
        result.append({"key": key, "primary": primary, "sheets": sheets})
    return result


def extract(cfg: PipelineConfig) -> None:
    conf = GoogleSheetsSettings.carregar()
    gc = gp.service_account(filename=str(conf.google_credentials_file))
    workbooks = _workbooks(cfg)
    total = sum(len(wb["sheets"]) for wb in workbooks)
    falhas = 0
    for wb in workbooks:
        url = conf.url_finance.get(wb["key"])
        if not url:
            logger.error(
                f"URL_FINANCE__{wb['key'].upper()} nao definida; planilha ignorada"
            )
            falhas += len(wb["sheets"])
            continue
        try:
            workbook = gc.open_by_url(url)
        except Exception:
            logger.exception(f"Falha ao abrir a planilha '{wb['key']}'")
            falhas += len(wb["sheets"])
            continue
        for sheet in wb["sheets"]:
            try:
                values = workbook.worksheet(sheet["name"]).get_all_values()
            except gp.exceptions.WorksheetNotFound:
                logger.warning(f"Aba '{sheet['name']}' nao encontrada em '{wb['key']}'")
                falhas += 1
                continue
            path = cfg.landing_dir / cfg.landing_file.format(
                workbook=wb["key"], sheet=sheet["stem"]
            )
            path.write_text(json.dumps(values, ensure_ascii=False), encoding="utf-8")
            logger.info(f"💾 {sheet['name']} -> {path.name} ({len(values)} linhas)")
    ensure_some_success(total, falhas, "aba(s)", log=logger)


def _dedupe_header(cells: list) -> list[str]:
    """Normaliza os nomes de coluna; nomeia vazios e desambigua duplicados."""
    header, seen = [], {}
    for i, cell in enumerate(cells):
        name = normalize_string(str(cell)) or f"col_{i}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        header.append(name)
    return header


def sheet_to_df(values: list[list], header_row: int) -> pd.DataFrame:
    """Linhas cruas da aba -> DataFrame com cabeçalho em ``header_row``."""
    if header_row >= len(values):
        return pd.DataFrame()
    df = pd.DataFrame(
        values[header_row + 1 :], columns=_dedupe_header(values[header_row])
    )
    return df[~(df == "").all(axis=1)]  # descarta linhas totalmente vazias


def transform(cfg: PipelineConfig) -> None:
    reset_bronze(cfg)
    for wb in _workbooks(cfg):
        for sheet in wb["sheets"]:
            path = cfg.landing_dir / cfg.landing_file.format(
                workbook=wb["key"], sheet=sheet["stem"]
            )
            if not path.exists():
                logger.warning(f"Sem landing para {path.name}; pulando")
                continue
            df = sheet_to_df(
                json.loads(path.read_text(encoding="utf-8")), sheet["header_row"]
            )
            if df.empty:
                logger.warning(f"Aba '{sheet['name']}' de '{wb['key']}' vazia; pulando")
                continue
            df["source_sheet"] = sheet["name"]
            df["source_workbook"] = wb["key"]
            write_csv(df, cfg.bronze_dir, f"{sheet['stem']}.csv", sep=cfg.bronze_sep)
