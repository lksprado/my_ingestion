"""Extract do solar: datas faltantes no Postgres -> login Selenium -> JSON por dia."""

import logging
from pathlib import Path

from core import HttpClient, PipelineConfig, PostgresClient, missing_dates_from_db
from pipelines.energia.solar._extraction import (
    login,
    navigate_to_report,
    session_headers,
    setup_driver,
)
from settings import settings

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "solar_config.yml"


def extract(cfg: PipelineConfig) -> None:
    opts = cfg.options
    sqls = [
        f"SELECT MAX({col})::date FROM {cfg.db_schema}.{table}"
        for table, col in opts["watermarks"].items()
    ]
    dates = missing_dates_from_db(
        PostgresClient(log=logger), sqls, cfg.landing_dir / opts["control_file"]
    )
    if not dates:
        logger.info("Nenhuma data faltando.")
        return

    driver = setup_driver(headless=bool(opts.get("headless", True)))
    try:
        login(
            driver,
            opts["login_url"],
            settings.apsystems_user,
            settings.apsystems_password,
        )
        navigate_to_report(driver)
        headers, user_id = session_headers(driver)
        http = HttpClient(logger, retries=3, backoff_factor=1.0, timeout=30)
        for day in dates:
            payload = {
                "selectedValue": opts["equipment_id"],
                "queryDate": day.replace("-", ""),
                "systemId": user_id,
                "userId": user_id,
            }
            data = http.request(
                opts["report_url"],
                method="POST",
                mode="json",
                data=payload,
                headers=headers,
            )
            http.save_json(data, cfg.landing_dir, cfg.landing_file.format(day=day))
    finally:
        driver.quit()
        logger.info("WebDriver encerrado")
