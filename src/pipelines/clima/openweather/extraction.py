"""Extração do day_summary do OpenWeather, um JSON por data."""

import logging
from datetime import date, datetime
from pathlib import Path

from core import HttpClient
from core.incremental import read_dates_csv

logger = logging.getLogger(__name__)


def read_missing(control_file: Path | str) -> list[str]:
    """Datas do arquivo de controle (compatível com o formato antigo, 1 por linha)."""
    return read_dates_csv(control_file)


def _as_str(d) -> str:
    if isinstance(d, datetime | date):
        return d.strftime("%Y-%m-%d")
    return datetime.strptime(str(d).strip(), "%Y-%m-%d").strftime("%Y-%m-%d")


def get_day_summary(
    output_path: Path | str,
    dates_list: list,
    token: str,
    lat: float,
    lon: float,
    base_url: str = "https://api.openweathermap.org/data/3.0/onecall/day_summary",
    filename_template: str = "day_summary_{day}.json",
) -> list[Path]:
    """Requisita o resumo de cada data e grava ``day_summary_<data>.json``.

    O token vai na query string, então a URL **não** é logada; só a data.
    """
    if not token:
        raise ValueError("Token da OpenWeather ausente (OPENWEATHER_API_KEY).")

    http = HttpClient(logger, retries=3, backoff_factor=1.0, timeout=15)
    saved: list[Path] = []
    for d in dates_list:
        day = _as_str(d)
        logger.info(f"GET day_summary {day}")
        data = http.make_http_request(
            base_url, params={"lat": lat, "lon": lon, "date": day, "appid": token}
        )
        path = http.save_json(data, output_path, filename_template.format(day=day))
        if path:
            saved.append(path)
    return saved
