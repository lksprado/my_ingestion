"""Contadores gerais das consultas públicas do e-Cidadania."""

import logging
from pathlib import Path

from core import GenericETL, PipelineConfig, run_cli
from pipelines.legislativo.ecidadania._common import fetch_to_csv, transform
from pipelines.legislativo.ecidadania._parsers import parse_big_numbers

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "ecidadania_config.yml"


def extract(cfg: PipelineConfig) -> None:
    fetch_to_csv(cfg, parse_big_numbers, cfg.url_base, cfg.landing_file)


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "bignumbers")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.ecidadania.ecidadania_bignumbers
