"""Orientações de bancada por votação (extração incremental por ID)."""

import logging
from functools import partial
from pathlib import Path

from core import GenericETL, PipelineConfig, run_cli, write_bronze
from pipelines.legislativo._common import (
    concat_landing,
    extract_by_ids,
    parse_dados_abertos,
)

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"


def transform(cfg: PipelineConfig) -> None:
    df = concat_landing(cfg, partial(parse_dados_abertos, url_col="url_votos"))
    write_bronze(cfg, df)


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "votos_orientacao")
    return GenericETL(
        cfg, extract_fn=extract_by_ids, transform_fn=transform, log=logger
    )


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.camara.camara_votos_orientacao
