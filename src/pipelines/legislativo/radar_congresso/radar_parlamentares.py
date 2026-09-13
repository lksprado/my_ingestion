"""Busca de parlamentares do Radar Congresso (JSON único -> tabela)."""

import logging
from pathlib import Path

import pandas as pd

from core import GenericETL, PipelineConfig, run_cli, write_bronze

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "radar_congresso_config.yml"


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, pd.read_json(cfg.landing_filepath, dtype=str))


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "parlamentares")
    return GenericETL(cfg, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.radar_congresso.radar_parlamentares
