"""Pipeline de scraping da Vide Editorial (ex-webscraping-books-refactor).

Extrai livros da home e das páginas de categoria (listadas em config.yml) para
JSONs em ``${LAKE_ROOT}/raw/vide/...`` e carrega no Postgres via
``PostgresClient.load_files_to_table``.
"""

from datetime import datetime
from pathlib import Path
from time import sleep
from zoneinfo import ZoneInfo

from my_ingestion.core import load_yaml, setup_logger
from my_ingestion.core.db import PostgresClient
from my_ingestion.core.http import HttpClient
from my_ingestion.pipelines.livros.vide_editorial.parsers import (
    get_last_page_number,
    parse_content_pages,
    parse_home_sales,
)
from my_ingestion.settings import settings

logger = setup_logger(__name__)

_CONFIG_FILE = Path(__file__).parent / "config.yml"
URL_BASE = "https://videeditorial.com.br/"


def _file_date() -> str:
    return datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")


def extraction_featured_books(output_dir: Path):
    output_dir = Path(output_dir)
    extract = HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
    html = extract.make_request(url=URL_BASE, mode="text")
    products = parse_home_sales(html)

    new_filename = f"vide_livros_em_destaque_{_file_date()}"
    extract.save_response(products, output_dir, new_filename)
    logger.info("Extraction complete!")


def extract_categories_content(
    output_dir: Path,
    config_file: Path = _CONFIG_FILE,
    only: str | None = None,
):
    """Extrai as páginas de categoria listadas no config.yml.

    Args:
        only: nome de uma única categoria para extrair (None = todas).
    """
    output_dir = Path(output_dir)
    extract = HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
    file_date = _file_date()

    hrefs = load_yaml(config_file).get("hrefs", [])
    if only is not None:
        hrefs = [h for h in hrefs if h.get("name") == only]
        if not hrefs:
            logger.error(f"Categoria '{only}' não encontrada no config.yml")
            return

    for item in hrefs:
        name = item.get("name")
        link = item.get("link")
        separator = "&" if "?" in link else "?"

        first_page = f"{link}{separator}page=1"
        html = extract.make_request(url=first_page, mode="text")
        last_page = get_last_page_number(html)

        extract.save_response(
            parse_content_pages(html), output_dir, f"{name}_page_1_{file_date}"
        )
        logger.info(f"Fetched data in {first_page} --- Total pages: {last_page}")
        sleep(1)

        for page_n in range(2, last_page + 1):
            link_page = f"{link}{separator}page={page_n}"
            html = extract.make_request(url=link_page, mode="text")
            extract.save_response(
                parse_content_pages(html),
                output_dir,
                f"{name}_page_{page_n}_{file_date}",
            )
            logger.info(f"Fetched data in {link_page} --- Total pages: {last_page}")
            sleep(1.2)

    logger.info("Extraction complete!")


def load(input_dir: Path, table_name: str) -> None:
    PostgresClient(log=logger).load_files_to_table(
        input_dir,
        table_name=table_name,
        file_extension="json",
        source_column="source_filename",
    )


if __name__ == "__main__":
    raw_dir = settings.lake_root / "raw" / "vide"
    extraction_featured_books(raw_dir)
    load(raw_dir, "vide_raw_livros_em_destaque")
    # python -m my_ingestion.pipelines.livros.vide_editorial.run
