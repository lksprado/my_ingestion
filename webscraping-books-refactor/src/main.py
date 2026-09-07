from datetime import datetime
from pathlib import Path
from time import sleep
from zoneinfo import ZoneInfo

import yaml

from .extractor import Extractor
from .loader import load_data
from .parsers import get_last_page_number, parse_content_pages, parse_home_sales
from .utils.log import logger


def extraction_featured_books(output_dir: Path):
    output_dir = Path(output_dir)

    url_base = "https://videeditorial.com.br/"
    extract = Extractor()
    html = extract.make_request(url=url_base, mode="text")
    products = parse_home_sales(html)

    file_date = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")
    new_filename = f"vide_livros_em_destaque_{file_date}"

    extract.save_json(data=products, output_dir=output_dir, filename=new_filename)
    logger.info("Extraction complete!")


def extract_link_content(output_dir: Path, config_file: Path):
    output_dir = Path(output_dir)
    config_file = Path(config_file)
    extract = Extractor()

    file_date = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")

    with open(config_file, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    hrefs = cfg.get("hrefs", [])

    for item in hrefs:
        name = item.get("name")
        link = item.get("link")
        separator = "&" if "?" in link else "?"
        first_page = f"{link}{separator}page=1"
        html = extract.make_request(url=first_page, mode="text")

        last_page = get_last_page_number(html)
        first_products = parse_content_pages(html)

        filename_base = f"{name}_page_1_{file_date}"
        extract.save_json(
            data=first_products, output_dir=output_dir, filename=filename_base
        )
        logger.info(f"Fetched data in {first_page} --- Total pages: {last_page}")
        sleep(1)
        if last_page > 1:
            for page_n in range(2, last_page + 1):
                link_page = f"{link}{separator}page={page_n}"
                html = extract.make_request(url=link_page, mode="text")
                products = parse_content_pages(html)
                filename = f"{name}_page_{page_n}_{file_date}"
                extract.save_json(
                    data=products, output_dir=output_dir, filename=filename
                )
                logger.info(f"Fetched data in {link_page} --- Total pages: {last_page}")
                sleep(1.2)

    logger.info("Extraction complete!")


def extract_one_category_content(name: str, output_dir: Path, config_file: Path):
    output_dir = Path(output_dir)
    config_file = Path(config_file)

    extract = Extractor()
    file_date = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")

    with open(config_file, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    hrefs = cfg.get("hrefs", [])
    item = next((h for h in hrefs if h.get("name") == name), None)

    if not item:
        logger.error(f"Categoria '{name}' não encontrada no config.yml")
        return

    link = item.get("link")

    # Adicionar page=1 preservando query params existentes
    separator = "&" if "?" in link else "?"
    first_page = f"{link}{separator}page=1"
    html = extract.make_request(url=first_page, mode="text")

    last_page = get_last_page_number(html)
    first_products = parse_content_pages(html)

    filename_base = f"{name}_page_1_{file_date}"
    extract.save_json(
        data=first_products, output_dir=output_dir, filename=filename_base
    )
    logger.info(f"Fetched data in {first_page} --- Total pages: {last_page}")
    sleep(1)

    if last_page > 1:
        for page_n in range(2, last_page + 1):
            link_page = f"{link}{separator}page={page_n}"
            html = extract.make_request(url=link_page, mode="text")
            products = parse_content_pages(html)
            filename = f"{name}_page_{page_n}_{file_date}"
            extract.save_json(data=products, output_dir=output_dir, filename=filename)
            logger.info(f"Fetched data in {link_page} --- Total pages: {last_page}")
            sleep(1.2)

    logger.info("Extraction complete!")


if __name__ == "__main__":
    # path = "/media/lucas/Files/2.Projetos/0.mylake/raw/vide"

    # extraction_featured_books(path)

    # load_data(
    #     dir=path,
    #     file_extension="json",
    #     schema="raw",
    #     table_name="vide_raw_livros_em_destaque",
    # )
    path = "/media/lucas/Files/2.Projetos/0.mylake/raw/vide/destaques"
    # extract_link_content(path)
    load_data(path, "json", "raw", "vide_raw_home_featured")
