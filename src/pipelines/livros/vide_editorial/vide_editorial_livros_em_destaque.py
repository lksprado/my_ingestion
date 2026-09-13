"""Livros em destaque na home da Vide Editorial (um JSON por dia no landing)."""

import logging
from pathlib import Path

import pandas as pd

from core import GenericETL, HttpClient, PipelineConfig, run_cli, write_bronze
from pipelines.livros.vide_editorial._parsers import parse_home_sales

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "vide_editorial_config.yml"


def extract(cfg: PipelineConfig) -> None:
    http = HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
    html = http.get_text(cfg.url_base)
    if html is None:
        return
    http.save_json(parse_home_sales(html), cfg.landing_dir, cfg.landing_file)


def transform(cfg: PipelineConfig) -> None:
    frames = []
    for f in sorted(cfg.landing_dir.glob(cfg.options["file_pattern"])):
        df = pd.read_json(f)
        df["source_filename"] = f.name
        frames.append(df)
    write_bronze(cfg, pd.concat(frames, ignore_index=True) if frames else None)


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "livros_em_destaque")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.livros.vide_editorial.vide_editorial_livros_em_destaque
