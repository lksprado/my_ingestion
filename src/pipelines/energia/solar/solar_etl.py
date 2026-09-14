"""ETL da energia solar (portal APsystems) -> CSVs que o Airflow carrega em
raw_apsystem (schema preservado do the_dw).

extract: high-water mark no Postgres -> datas faltantes -> login Selenium -> um
JSON horário por dia no landing. ``daily_energy`` e ``hourly_energy`` leem o mesmo
landing; só o primeiro extrai. ``load: none``: o Airflow faz o upsert.
"""

import logging
import re
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import Select, WebDriverWait

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
CONFIG_FILE = Path(__file__).parent / "solar_config.yml"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)


def setup_driver(
    headless: bool = True, remote_url: str | None = None
) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-agent={USER_AGENT}")
    if remote_url:
        logger.info("Iniciando WebDriver remoto")
        return webdriver.Remote(command_executor=remote_url, options=options)
    logger.info("Iniciando WebDriver local")
    return webdriver.Chrome(options=options)


def login(driver: webdriver.Chrome, url: str, username: str, password: str) -> None:
    logger.info("Realizando login")
    driver.get(url)
    wait = WebDriverWait(driver, 30)
    wait.until(ec.presence_of_element_located((By.ID, "username"))).send_keys(username)
    wait.until(ec.presence_of_element_located((By.ID, "password"))).send_keys(password)
    wait.until(ec.element_to_be_clickable((By.ID, "Login"))).click()
    wait.until(ec.presence_of_element_located((By.ID, "report_head")))
    logger.info("Login realizado com sucesso")


def navigate_to_report(driver: webdriver.Chrome) -> None:
    logger.info("Navegando até relatório")
    wait = WebDriverWait(driver, 30)
    for element_id in ("report_head", "systemDataCustomer", "ecuData"):
        el = wait.until(ec.presence_of_element_located((By.ID, element_id)))
        driver.execute_script("arguments[0].click();", el)
    # O portal passou a abrir a aba ECU Data num iframe próprio (tab_iframe_<n>,
    # identificado pelo src); o configuration_body ficou só no layout antigo.
    wait.until(
        ec.frame_to_be_available_and_switch_to_it(
            (
                By.CSS_SELECTOR,
                "iframe[src*='intoSysAndECULevel'], iframe#configuration_body",
            )
        )
    )
    Select(
        wait.until(ec.presence_of_element_located((By.ID, "chart")))
    ).select_by_value("2")
    driver.switch_to.default_content()
    logger.info("Relatório configurado")


def session_headers(driver: webdriver.Chrome) -> tuple[dict, str]:
    """``(headers com Cookie, userId)`` da sessão logada, para as chamadas AJAX."""
    cookies = driver.get_cookies()
    user_id = next(c["value"] for c in cookies if c["name"] == "userId")
    headers = {
        "Cookie": "; ".join(f"{c['name']}={c['value']}" for c in cookies),
        "User-Agent": USER_AGENT,
    }
    return headers, user_id


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

    driver = setup_driver(
        headless=bool(opts.get("headless", True)),
        remote_url=settings.selenium_remote_url,
    )
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


# ------------------------------ parsers ------------------------------

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_hourly_json(path: Path) -> pd.DataFrame:
    """Um arquivo (um dia): uma linha por hora, com ``date`` vinda do nome."""
    df = pd.read_json(path).reset_index().rename(columns={"index": "hour"})
    df["filename"] = path.name
    df["date"] = _DATE_RE.search(path.name).group(0)
    return df


def load_landing(cfg: PipelineConfig) -> pd.DataFrame:
    """Concatena todos os JSONs do landing (arquivo inválido é pulado)."""
    frames = []
    for f in sorted(cfg.landing_dir.glob(cfg.landing_file.format(day="*"))):
        try:
            frames.append(parse_hourly_json(f))
        except Exception as e:
            logger.warning(f"JSON vazio ou inválido {f} -- {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por dia: duração, total, CO2 e pico."""
    if df.empty:
        return df
    out = df[["date", "duration", "total", "co2", "max"]].drop_duplicates(
        subset=["date"]
    )
    out = out.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out


def hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por data-hora com a energia gerada."""
    if df.empty:
        return df
    out = df.copy()
    out["datetime"] = pd.to_datetime(out["date"]) + pd.to_timedelta(
        out["hour"], unit="h"
    )
    return out[["datetime", "energy"]].drop_duplicates(subset=["datetime"])


def transform_daily(cfg: PipelineConfig) -> None:
    write_bronze(cfg, daily_summary(load_landing(cfg)))


def transform_hourly(cfg: PipelineConfig) -> None:
    write_bronze(cfg, hourly(load_landing(cfg)))


ETLS = {
    "daily_energy": Etl(extract=extract, transform=transform_daily),
    "hourly_energy": Etl(transform=transform_hourly),  # usa o landing de daily_energy
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.energia.solar.solar_etl [entidade ...] [--steps ...]
