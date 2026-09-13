"""High-water mark do solar: descobre as datas ainda não carregadas no Postgres.

As funções públicas mantêm a assinatura usada pelo ``run.py`` e pelo DAG do
Airflow (``identify_missing_dates(db)`` aceita conexão psycopg2 ou PostgresHook).
"""

import logging

from core.incremental import get_max_date, missing_dates, write_dates_csv

logger = logging.getLogger(__name__)

SCHEMA = "raw_solar"
_QUERY_DAILY = f"SELECT MAX(date) :: DATE AS DT FROM {SCHEMA}.solar_daily_energy"
_QUERY_HOURLY = f"SELECT MAX(datetime) :: DATE AS DT FROM {SCHEMA}.solar_hourly_energy"


def identify_missing_dates(db) -> list[str]:
    """Datas faltantes a partir do menor high-water mark entre diário e horário."""
    logger.info("Obtendo data maxima no DW")
    max_daily = get_max_date(db, _QUERY_DAILY)
    max_hourly = get_max_date(db, _QUERY_HOURLY)
    if max_daily is None or max_hourly is None:
        raise ValueError(f"Tabelas {SCHEMA}.solar_* vazias: sem high-water mark.")

    start_date = min(max_daily, max_hourly)
    logger.info(f"Data inicial encontrada: {start_date}")

    dates = missing_dates(start_date)
    if not dates:
        logger.info("Nenhuma data faltando.")
    else:
        logger.info(f"{len(dates)} data(s) faltando: {dates[0]} .. {dates[-1]}")
    return dates


def write_list_to_csv(input_ls: list, output_filepath) -> None:
    write_dates_csv(input_ls, output_filepath)


def identify_and_write_missing_dates(db, output_filepath) -> list[str]:
    """Calcula as datas faltantes e grava o arquivo de controle (vazio se ok)."""
    dates = identify_missing_dates(db=db)
    write_dates_csv(dates, output_filepath)
    return dates
