"""Pipeline de energia solar (ex-projeto solar).

Descobre datas faltantes no Postgres (high-water mark), extrai a produção do
portal apsystemsema.com via Selenium e transforma os JSONs em CSVs diário/horário
no staging do lake.
"""

import csv
import os
from pathlib import Path

from my_ingestion.core import setup_logger
from my_ingestion.core.db import PostgresClient
from my_ingestion.pipelines.energia.solar.extraction import (
    Credentials,
    PathsConfig,
    SeleniumConfig,
    run_pipeline,
)
from my_ingestion.pipelines.energia.solar.missing_raw import (
    identify_and_write_missing_dates,
)
from my_ingestion.pipelines.energia.solar.transforming import (
    make_daily_summary_df,
    make_hourly_df,
    parsing_json_to_dataframe,
)
from my_ingestion.settings import settings

logger = setup_logger(__name__)

if __name__ == "__main__":
    staging = settings.lake_root / "staging" / "solar_project"
    control = staging / "missing_dates.csv"
    staging.mkdir(parents=True, exist_ok=True)

    db_con = PostgresClient(log=logger)._connect()
    try:
        identify_and_write_missing_dates(db=db_con, output_filepath=control)
    finally:
        db_con.close()

    dates = []
    if os.path.exists(control):
        with open(control) as f:
            reader = csv.reader(f)
            dates = [row[0] for row in reader if row]

    if dates:
        creds_obj = Credentials(
            username=settings.apsystems_user, password=settings.apsystems_password
        )
        selenium_config = SeleniumConfig(remote_url=None, headless=False)
        paths = PathsConfig(staging_dir=Path(staging))

        run_pipeline(
            selenium_config=selenium_config, creds=creds_obj, paths=paths, dates=dates
        )

        df = parsing_json_to_dataframe(staging)
        daily = make_daily_summary_df(df)
        hourly = make_hourly_df(df)
    else:
        logger.info("No missing dates to process.")
    # python -m my_ingestion.pipelines.energia.solar.run
