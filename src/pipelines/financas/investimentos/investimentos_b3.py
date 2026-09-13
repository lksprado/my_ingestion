"""Relatórios Excel da B3 (uma tabela por aba, consolidando todas as pessoas).

Sem extract: os arquivos são colocados manualmente em raw/investments/b3/<pessoa>/.
"""

import logging
import warnings
from collections import defaultdict
from pathlib import Path

import pandas as pd

from core import (
    GenericETL,
    PipelineConfig,
    list_files,
    normalize_string,
    run_cli,
    write_csv,
)
from pipelines.financas.investimentos._common import reset_bronze

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "investimentos_config.yml"

# Abas com nomes diferentes mas mesmo schema, consolidadas sob um nome só.
ALIASES = {"proventos_recebidos": "proventos"}


def _read_sheet(file: Path, sheet: str) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)  # openpyxl: estilos desconhecidos
        df = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
    return df.dropna(subset=[df.columns[0]])


def transform(cfg: PipelineConfig) -> None:
    """Explode as abas de todas as planilhas e grava um CSV por aba no bronze."""
    reset_bronze(cfg)
    files = [
        f for f in list_files(cfg.landing_dir) if f.suffix.lower() in (".xlsx", ".xls")
    ]
    logger.info(f"Consolidando {len(files)} planilha(s) de {cfg.landing_dir}")

    grouped: dict[str, list[pd.DataFrame]] = defaultdict(list)
    for f in files:
        for sheet in pd.ExcelFile(f, engine="openpyxl").sheet_names:
            name = normalize_string(str(sheet)).removeprefix("posicao_")
            df = _read_sheet(f, sheet)
            df.columns = df.columns.map(normalize_string)
            df["source_path"] = str(f)  # inclui a pasta da pessoa
            grouped[ALIASES.get(name, name)].append(df)

    for name, dfs in grouped.items():
        df = pd.concat(dfs, ignore_index=True)
        write_csv(df, cfg.bronze_dir, f"{name}.csv", sep=cfg.bronze_sep)
        logger.info(f"{name}: {len(dfs)} aba(s) -> {len(df)} linhas")


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "b3")
    return GenericETL(cfg, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.financas.investimentos.investimentos_b3
