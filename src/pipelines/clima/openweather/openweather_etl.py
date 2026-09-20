"""ETL do resumo diário do clima (OpenWeather day_summary), incremental por data.

extract: high-water mark em raw_openweather.openweather_daily -> datas faltantes
(CSV de controle) -> um JSON por dia no landing, que acumula o histórico.
transform: todos os JSONs do landing -> all_dfs.csv. load: full refresh
(``write: truncate``) — a tabela é função do landing, e é o extract que evita
rebaixar o que já está lá.
"""

import json
import logging
from pathlib import Path

import pandas as pd

from core import (
    Etl,
    HttpClient,
    PipelineConfig,
    PostgresClient,
    missing_dates_from_db,
    run_source,
    write_bronze,
)
from settings import settings

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "openweather_config.yml"

_TEMPERATURE_COLS = [
    "temperature_min",
    "temperature_max",
    "temperature_afternoon",
    "temperature_night",
    "temperature_evening",
    "temperature_morning",
]
_INT_COLS = [
    "cloud_cover_afternoon",
    "humidity_afternoon",
    "precipitation_total",
    "wind_max_direction",
    "pressure_afternoon",
]
_DROP_COLS = ["tz", "units"]


def parse_day_summary(content: dict) -> pd.DataFrame:
    """Achata um day_summary (uma linha) e converte Kelvin -> Celsius."""
    df = pd.json_normalize(content)
    df.columns = [c.replace(".", "_") for c in df.columns]
    for col in _TEMPERATURE_COLS:
        df[col] = (df[col].astype(float) - 273.15).round(2)
    for col in _INT_COLS:
        df[col] = df[col].astype(int)
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    return df.drop(columns=[c for c in _DROP_COLS if c in df.columns])


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
    if settings.openweather_lat is None or settings.openweather_lon is None:
        raise ValueError("Ponto ausente (OPENWEATHER_LAT / OPENWEATHER_LON).")

    http = HttpClient(logger, retries=3, backoff_factor=1.0, timeout=15)
    for day in dates:
        logger.info(f"GET day_summary {day}")  # token na query string: URL não é logada
        data = http.get_json(
            cfg.url_base,
            params={
                "lat": settings.openweather_lat,
                "lon": settings.openweather_lon,
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


ETLS = {"daily": Etl(extract=extract, transform=transform)}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.clima.openweather.openweather_etl [--steps transform]
