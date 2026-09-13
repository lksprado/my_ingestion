"""Senadores em exercício (extract padrão: base_url -> landing_file)."""

import json
import logging
from pathlib import Path

import pandas as pd

from core import (
    GenericETL,
    PipelineConfig,
    run_cli,
    sanitize_columns,
    sanitize_values,
    write_bronze,
)
from pipelines.legislativo._common import concat_landing

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"
_RECORD_PATH = ["ListaParlamentarEmExercicio", "Parlamentares", "Parlamentar"]
_KEEP_VALUES = [
    "identificacaoparlamentar_urlfotoparlamentar",
    "identificacaoparlamentar_urlpaginaparlamentar",
    "mandato_suplentes_suplente",
    "mandato_exercicios_exercicio",
    "identificacaoparlamentar_telefones_telefone",
    "identificacaoparlamentar_emailparlamentar",
    "identificacaoparlamentar_urlpaginaparticular",
]


def _parse(path: Path) -> pd.DataFrame:
    with open(path, encoding="utf-8") as fp:
        df = pd.json_normalize(json.load(fp), record_path=_RECORD_PATH, sep=".")
    return sanitize_values(sanitize_columns(df), exclude=_KEEP_VALUES)


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, _parse))


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "senadores")
    return GenericETL(cfg, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.senado.senado_senadores
