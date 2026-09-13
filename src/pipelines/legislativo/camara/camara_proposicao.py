"""Detalhe de cada proposição (extração incremental por ID, bronze em streaming)."""

import logging
from functools import partial
from pathlib import Path

from core import GenericETL, PipelineConfig, run_cli, write_bronze_streaming
from pipelines.legislativo._common import extract_by_ids, parse_dados_abertos

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"


def transform(cfg: PipelineConfig) -> None:
    write_bronze_streaming(
        cfg,
        sorted(cfg.landing_dir.glob("*.json")),
        partial(parse_dados_abertos, url_col="urls"),
    )


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "proposicao")
    return GenericETL(
        cfg, extract_fn=extract_by_ids, transform_fn=transform, log=logger
    )


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.camara.camara_proposicao
