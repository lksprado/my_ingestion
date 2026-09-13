"""Resumo diário do clima (OpenWeather day_summary), incremental por data.

extract: high-water mark em raw_openweather.openweather_daily -> datas faltantes
(CSV de controle) -> um JSON por dia no landing. transform: JSONs -> all_dfs.csv.
Sem load: o Airflow faz o upsert.
"""

import json
import logging
from pathlib import Path

import pandas as pd

from core import (
    GenericETL,
    HttpClient,
    PipelineConfig,
    PostgresClient,
    missing_dates_from_db,
    run_cli,
    write_bronze,
)
from pipelines.clima.openweather._parsers import parse_day_summary
from settings import settings

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "openweather_config.yml"


def extract(cfg: PipelineConfig) -> None:
    opts = cfg.options
    sql = f"SELECT MAX({opts['date_column']})::date FROM {cfg.db_schema}.{cfg.db_table}"
    dates = missing_dates_from_db(
        PostgresClient(log=logger), [sql], cfg.landing_dir / opts["control_file"]
    )
    if not dates:
        logger.info("Nenhuma data faltando.")
        return
    if not settings.openweather_api_key:
        raise ValueError("Token da OpenWeather ausente (OPENWEATHER_API_KEY).")

    http = HttpClient(logger, retries=3, backoff_factor=1.0, timeout=15)
    for day in dates:
        logger.info(f"GET day_summary {day}")  # token na query string: URL não é logada
        data = http.get_json(
            cfg.url_base,
            params={
                "lat": opts["lat"],
                "lon": opts["lon"],
                "date": day,
                "appid": settings.openweather_api_key,
            },
        )
        http.save_json(data, cfg.landing_dir, cfg.landing_file.format(day=day))


def transform(cfg: PipelineConfig) -> None:
    frames = []
    for f in sorted(cfg.landing_dir.glob(cfg.landing_file.format(day="*"))):
        try:
            with open(f, encoding="utf-8") as fp:
                frames.append(parse_day_summary(json.load(fp)))
        except Exception as e:
            logger.warning(f"JSON vazio ou inválido {f} -- {e}")
    write_bronze(cfg, pd.concat(frames, ignore_index=True) if frames else None)


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "daily")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.clima.openweather.openweather_daily [--steps transform]
