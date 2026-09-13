"""Todas as páginas de consultas públicas (``options.pages``); bronze ganha total_votos."""

import logging
from pathlib import Path

import pandas as pd

from core import GenericETL, HttpClient, PipelineConfig, run_cli, write_bronze
from pipelines.legislativo._common import concat_landing
from pipelines.legislativo.ecidadania._common import _read_csv, fetch_to_csv
from pipelines.legislativo.ecidadania._parsers import parse_materias

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "ecidadania_config.yml"


def extract(cfg: PipelineConfig) -> None:
    http = HttpClient(logger)
    for page in range(1, int(cfg.options.get("pages", 145)) + 1):
        logger.info(f"Extraindo pagina {page}...")
        fetch_to_csv(
            cfg,
            parse_materias,
            f"{cfg.url_base}{page}",
            cfg.landing_file.format(page=page),
            http=http,
        )


def transform(cfg: PipelineConfig) -> None:
    df = concat_landing(cfg, _read_csv, pattern="*.csv")
    if not df.empty:
        df["total_votos"] = pd.to_numeric(df["votos_sim"], errors="coerce") + (
            pd.to_numeric(df["votos_nao"], errors="coerce")
        )
    write_bronze(cfg, df)


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "paginas")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.ecidadania.ecidadania_paginas
