"""High-water mark do clima: datas ainda não carregadas em raw.openweather_daily.

``identify_missing_dates(db)`` aceita conexão psycopg2 ou PostgresHook (Airflow).
"""

import logging

from core.incremental import get_max_date, missing_dates, write_dates_csv

logger = logging.getLogger(__name__)

_QUERY_DAILY = "SELECT MAX(date) :: DATE AS DT FROM raw.openweather_daily"


def identify_missing_dates(db) -> list[str]:
    logger.info("Obtendo data maxima no DW")
    max_date = get_max_date(db, _QUERY_DAILY)
    if max_date is None:
        raise ValueError("raw.openweather_daily vazia: sem high-water mark.")
    logger.info(f"Data inicial encontrada: {max_date}")

    dates = missing_dates(max_date)
    if not dates:
        logger.info("Nenhuma data faltando.")
    else:
        logger.info(f"{len(dates)} data(s) faltando: {dates[0]} .. {dates[-1]}")
    return dates


def identify_and_write_missing_dates(db, output_filepath) -> list[str]:
    dates = identify_missing_dates(db=db)
    write_dates_csv(dates, output_filepath)
    return dates
