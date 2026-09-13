"""ETL do Ranking dos Políticos -> ``raw_ranking_politicos.<entidade>``.

Um JSON por ano no landing (``{year}`` em ``landing_file``); o bronze concatena
todos os anos.
"""

import logging
from datetime import date
from pathlib import Path

from core import (
    Etl,
    HttpClient,
    PipelineConfig,
    concat_landing,
    run_source,
    write_bronze,
)
from core.parsers.json import normalize_json_object

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "ranking_politicos_config.yml"


def extract(cfg: PipelineConfig) -> None:
    filename = cfg.landing_file.format(year=date.today().year)
    HttpClient(logger).fetch_and_save(cfg.url_base, cfg.landing_dir, filename)


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, lambda f: normalize_json_object(f, "items")))


ETLS = {
    "deputados": Etl(extract=extract, transform=transform),
    "senadores": Etl(extract=extract, transform=transform),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.legislativo.ranking_politicos.ranking_politicos_etl [entidade ...] [--steps ...]
