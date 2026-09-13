"""Selenium do portal APsystems: login, navegação até o relatório e cookies."""

import logging

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import Select, WebDriverWait

logger = logging.getLogger(__name__)

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
    wait.until(ec.frame_to_be_available_and_switch_to_it((By.ID, "configuration_body")))
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
