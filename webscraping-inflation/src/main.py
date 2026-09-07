import re
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

from .extractor import Extractor
from .scraper import AtacadaoScraper
from .utils.log import logger


def clean_word(word: str):
    word = word.lower()

    # remove acentos
    word = unicodedata.normalize("NFKD", word)
    word = "".join(c for c in word if not unicodedata.combining(c))

    # troca espaços por underscore
    word = re.sub(r"\s+", "_", word.strip())

    return word


def load_yaml(path: Path) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_keywords(products_cfg_path: Path | None) -> list[str]:
    if not products_cfg_path or not Path(products_cfg_path).exists():
        return []

    raw = load_yaml(products_cfg_path).get("keywords", [])
    keywords = []
    for item in raw:
        if isinstance(item, dict) and "name" in item:
            keywords.append(item["name"])
        else:
            keywords.append(str(item))
    return keywords


def search_products(
    store_config_file: Path,
    output_dir: Path,
    products_config_file: Path | None = None,
):
    extractor = Extractor()
    output_dir = Path(output_dir)

    store_cfg = load_yaml(store_config_file)
    stores = [s for s in store_cfg.get("stores", []) if s.get("enabled", True)]
    if not stores:
        logger.warning("Nenhuma loja encontrada em %s", store_config_file)
        return

    keywords = load_keywords(products_config_file)
    if not keywords:
        logger.warning("Nenhuma keyword encontrada")
        return

    scrapers = [AtacadaoScraper(store, extractor) for store in stores]

    dt = datetime.now().strftime("%Y-%m-%d")
    for scraper in scrapers:
        for keyword in keywords:
            products = scraper.search(keyword)
            if not products:
                continue

            df = pd.DataFrame(products)
            df["keyword"] = keyword
            df["extracted_at"] = dt

            keyword_clean = clean_word(keyword)
            filepath = output_dir / f"{scraper.store_id}_{keyword_clean}_{dt}.csv"
            df.to_csv(filepath, sep=";", index=False)
            logger.info("Saved %d rows to %s", len(df), filepath)


if __name__ == "__main__":
    output = "/media/lucas/Files/2.Projetos/0.mylake/raw/inflation/atacadao"

    store_config = "src/store_config.yml"
    products_config = "src/products_config.yml"

    search_products(
        store_config_file=store_config,
        products_config_file=products_config,
        output_dir=output,
    )
