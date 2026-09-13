"""Governismo trimestral dos senadores (Radar Congresso)."""

import logging
from pathlib import Path

from core import GenericETL, PipelineConfig, run_cli
from pipelines.legislativo.radar_congresso._common import transform_governismo

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "radar_congresso_config.yml"


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "governismo_senadores")
    return GenericETL(cfg, transform_fn=transform_governismo, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.radar_congresso.radar_governismo_senadores
