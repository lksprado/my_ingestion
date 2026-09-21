"""Pipeline de preços do Atacadão.

Busca produtos via API GraphQL por keyword e grava CSVs diários no lake:
``${LAKE_ROOT}/raw/inflation/atacadao``.
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from core import HttpClient, load_yaml, normalize_string, setup_logger
from pipelines.precos.atacadao.scraper import AtacadaoScraper
from settings import settings

logger = logging.getLogger(__name__)

_STORE_CONFIG = Path(__file__).parent / "store_config.yml"
_PRODUCTS_CONFIG = Path(__file__).parent / "products_config.yml"


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
    extractor = HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

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

            keyword_clean = normalize_string(keyword)
            filepath = output_dir / f"{scraper.store_id}_{keyword_clean}_{dt}.csv"
            df.to_csv(filepath, sep=";", index=False)
            logger.info("Saved %d rows to %s", len(df), filepath)


if __name__ == "__main__":
    setup_logger()
    search_products(
        store_config_file=_STORE_CONFIG,
        products_config_file=_PRODUCTS_CONFIG,
        output_dir=settings.lake_root / "raw" / "inflation" / "atacadao",
    )
    # uv run python -m pipelines.precos.atacadao.run
