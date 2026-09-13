"""Extract/transform do Ranking dos Políticos (um JSON por ano no landing)."""

import logging
from datetime import date
from pathlib import Path

from core import GenericETL, HttpClient, PipelineConfig, write_bronze
from core.parsers.json import normalize_json_object
from pipelines.legislativo._common import concat_landing

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "ranking_politicos_config.yml"


def extract(cfg: PipelineConfig) -> None:
    filename = cfg.landing_file.format(year=date.today().year)
    HttpClient(logger).fetch_and_save(cfg.url_base, cfg.landing_dir, filename)


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, lambda f: normalize_json_object(f, "items")))


def build(source: str) -> GenericETL:
    cfg = PipelineConfig.from_yaml(CONFIG_FILE, source)
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)
