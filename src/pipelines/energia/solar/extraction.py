import datetime
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import Select, WebDriverWait

# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# CONFIG DATA CLASSES (SÓ DADOS)
# ---------------------------------------------------------------------
@dataclass
class SeleniumConfig:
    remote_url: str | None = None
    headless: bool = True
    window_size: str = "1920,1080"
    user_agent: str | None = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    )


@dataclass
class Credentials:
    username: str
    password: str


@dataclass
class PathsConfig:
    staging_dir: Path


# ---------------------------------------------------------------------
# WEBDRIVER
# ---------------------------------------------------------------------
def setup_driver(config: SeleniumConfig) -> webdriver.Chrome:
    chrome_options = Options()

    if config.headless:
        chrome_options.add_argument("--headless=new")

    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"--window-size={config.window_size}")

    if config.user_agent:
        chrome_options.add_argument(f"--user-agent={config.user_agent}")

    if config.remote_url:
        logger.info("Iniciando WebDriver remoto")
        return webdriver.Remote(
            command_executor=config.remote_url,
            options=chrome_options,
        )

    logger.info("Iniciando WebDriver local")
    return webdriver.Chrome(options=chrome_options)


# ---------------------------------------------------------------------
# LOGIN
# ---------------------------------------------------------------------
def login(driver: webdriver.Chrome, creds: Credentials):
    logger.info("Realizando login")

    driver.get("https://apsystemsema.com/ema/index.action")
    wait = WebDriverWait(driver, 30)

    wait.until(ec.presence_of_element_located((By.ID, "username"))).send_keys(
        creds.username
    )
    wait.until(ec.presence_of_element_located((By.ID, "password"))).send_keys(
        creds.password
    )
    wait.until(ec.element_to_be_clickable((By.ID, "Login"))).click()

    # Confirma login bem-sucedido
    wait.until(ec.presence_of_element_located((By.ID, "report_head")))
    logger.info("Login realizado com sucesso")


# ---------------------------------------------------------------------
# NAVEGAÇÃO ATÉ O RELATÓRIO
# ---------------------------------------------------------------------
def navigate_to_report(driver: webdriver.Chrome):
    logger.info("Navegando até relatório")

    wait = WebDriverWait(driver, 30)

    report = wait.until(ec.presence_of_element_located((By.ID, "report_head")))
    driver.execute_script("arguments[0].click();", report)

    system_data = wait.until(
        ec.presence_of_element_located((By.ID, "systemDataCustomer"))
    )
    driver.execute_script("arguments[0].click();", system_data)

    ecu_data = wait.until(ec.presence_of_element_located((By.ID, "ecuData")))
    driver.execute_script("arguments[0].click();", ecu_data)

    wait.until(ec.frame_to_be_available_and_switch_to_it((By.ID, "configuration_body")))

    Select(
        wait.until(ec.presence_of_element_located((By.ID, "chart")))
    ).select_by_value("2")

    driver.switch_to.default_content()
    logger.info("Relatório configurado")


# ---------------------------------------------------------------------
# EXTRAÇÃO VIA AJAX
# ---------------------------------------------------------------------
def fetch_production_data(
    driver: webdriver.Chrome,
    query_date: str,
    output_dir: Path,
):
    cookies = driver.get_cookies()

    user_id = next(c["value"] for c in cookies if c["name"] == "userId")

    headers = {
        "Cookie": "; ".join(f"{c['name']}={c['value']}" for c in cookies),
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    payload = {
        "selectedValue": "216200001531",
        "queryDate": query_date,
        "systemId": user_id,
        "userId": user_id,
    }

    url = "https://apsystemsema.com/ema/ajax/getReportApiAjax/getHourlyEnergyOnCurrentDayAjax"

    response = requests.post(url, headers=headers, data=payload)
    response.raise_for_status()

    file_date = f"{query_date[:4]}-{query_date[4:6]}-{query_date[6:]}"
    output_file = output_dir / f"hourly24_production_{file_date}.json"

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(response.json(), ensure_ascii=False))

    logger.info(f"Extração salva em {output_file}")


# ---------------------------------------------------------------------
# PIPELINE ORQUESTRADOR
# ---------------------------------------------------------------------
def run_pipeline(
    selenium_config: SeleniumConfig,
    creds: Credentials,
    paths: PathsConfig,
    dates: list[str],
):
    driver = setup_driver(selenium_config)
    failed_dates = []

    try:
        login(driver, creds)
        navigate_to_report(driver)

        for day in dates:
            try:
                query_date = datetime.datetime.strptime(day, "%Y-%m-%d").strftime(
                    "%Y%m%d"
                )

                logger.info(f"Extraindo dados de {query_date}")
                fetch_production_data(driver, query_date, paths.staging_dir)
            except Exception:
                failed_dates.append(day)
                logger.exception(
                    "Falha ao extrair dados de %s. Pulando para a próxima data.",
                    day,
                )

    finally:
        driver.quit()
        logger.info("WebDriver encerrado")

    if failed_dates:
        logger.warning("Datas não extraídas: %s", ", ".join(failed_dates))

    return failed_dates


# ---------------------------------------------------------------------
# EXECUÇÃO LOCAL
# ---------------------------------------------------------------------
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    creds = Credentials(
        username=os.getenv("APSYSTEMS_USER"),
        password=os.getenv("APSYSTEMS_PASSWORD"),
    )

    selenium_config = SeleniumConfig(
        remote_url=None,  # usar Selenium local
        headless=False,
    )

    paths = PathsConfig(
        staging_dir=Path("./staging"),
    )

    dates = [
        "2025-12-07",
        "2025-12-08",
        "2025-12-09",
        "2025-12-10",
        "2025-12-11",
        "2025-12-12",
        "2025-12-13",
        "2025-12-14",
        "2025-12-15",
        "2025-12-16",
        "2025-12-17",
        "2025-12-18",
        "2025-12-19",
        "2025-12-20",
    ]

    run_pipeline(
        selenium_config=selenium_config,
        creds=creds,
        paths=paths,
        dates=dates,
    )
