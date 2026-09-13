"""Detalhe de cada processo (extração incremental por ID, bronze em streaming)."""

import json
import logging
from functools import partial
from pathlib import Path

import pandas as pd

from core import GenericETL, PipelineConfig, run_cli, write_bronze_streaming
from pipelines.legislativo._common import extract_by_ids

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"


def _parse(path: Path) -> pd.DataFrame | None:
    """Array na raiz; ``id_processo`` (id consultado) vem do nome do arquivo."""
    with open(path, encoding="utf-8") as fp:
        raw = json.load(fp)
    if not raw:
        return None
    df = pd.json_normalize(raw, sep=".")
    df["id_processo"] = path.stem.removesuffix("_processo")
    return df


def transform(cfg: PipelineConfig) -> None:
    write_bronze_streaming(cfg, sorted(cfg.landing_dir.glob("*.json")), _parse)


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "processo")
    return GenericETL(
        cfg,
        extract_fn=partial(extract_by_ids, has_data=bool),
        transform_fn=transform,
        log=logger,
    )


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.senado.senado_processo
