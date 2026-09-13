"""Energia solar horária (APsystems): hourly_energy.csv a partir do landing do daily."""

import logging

from core import GenericETL, PipelineConfig, run_cli, write_bronze
from pipelines.energia.solar._common import CONFIG_FILE
from pipelines.energia.solar._parsers import hourly, load_landing

logger = logging.getLogger(__name__)


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, hourly(load_landing(cfg)))


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(CONFIG_FILE, "hourly_energy")
    return GenericETL(cfg, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.energia.solar.solar_hourly_energy
