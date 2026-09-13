"""Energia solar diária (APsystems): extract incremental por data + daily_energy.csv."""

import logging

from core import GenericETL, PipelineConfig, run_cli, write_bronze
from pipelines.energia.solar._common import CONFIG_FILE, extract
from pipelines.energia.solar._parsers import daily_summary, load_landing

logger = logging.getLogger(__name__)


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, daily_summary(load_landing(cfg)))


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(CONFIG_FILE, "daily_energy")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.energia.solar.solar_daily_energy [--steps transform]
