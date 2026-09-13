"""Senadores da legislatura atual (extract padrão: base_url -> landing_file)."""

import json
import logging
from pathlib import Path

import pandas as pd

from core import GenericETL, PipelineConfig, run_cli, write_bronze
from pipelines.legislativo._common import concat_landing

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"
_RECORD_PATH = ["ListaParlamentarLegislatura", "Parlamentares", "Parlamentar"]


def _parse(path: Path) -> pd.DataFrame:
    with open(path, encoding="utf-8") as fp:
        return pd.json_normalize(json.load(fp), record_path=_RECORD_PATH, sep=".")


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, _parse))


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "legislaturas")
    return GenericETL(cfg, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.senado.senado_legislaturas
