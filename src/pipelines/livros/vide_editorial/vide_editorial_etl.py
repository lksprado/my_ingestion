"""ETL da Vide Editorial (scraping) -> ``raw_vide_editorial.<entidade>``.

O extract parseia o HTML e grava no landing a lista de produtos em JSON.
``livros_em_destaque`` (home) vai até o banco; ``categorias`` é extract-only
(``load: none``, sem consumidor no momento).
"""

import logging
from datetime import datetime
from pathlib import Path
from time import sleep
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup

from core import Etl, HttpClient, PipelineConfig, run_source, write_bronze

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "vide_editorial_config.yml"
BASE_URL = "https://videeditorial.com.br/"


def get_last_page_number(response: str) -> int:
    """Obtem ultima paginacao"""
    soup = BeautifulSoup(response, "html.parser")

    container = soup.select_one("div.pagination.bottom div.links")

    if not container:
        # não tem paginação → só 1 página
        return 1

    page_numbers = []

    for a in container.select("a[href]"):
        href = a["href"]

        # exemplo: /filosofia?page=37
        if "page=" in href:
            qs = parse_qs(urlparse(href).query)
            page = qs.get("page", [None])[0]

            if page and page.isdigit():
                page_numbers.append(int(page))

    if not page_numbers:
        return 1

    return max(page_numbers)


def parse_products_page(
    response: str,
    *,
    container_selector: str,
    item_selector: str,
    source: str,
    has_category: bool = False,
) -> list:
    """Parser genérico de lista de produtos"""
    soup = BeautifulSoup(response, "html.parser")
    products = []

    # Categoria (só para páginas de categoria)
    category_name = None
    if has_category:
        category_tag = soup.select_one("#column-right .back-category a")
        category_name = category_tag.get_text(strip=True) if category_tag else None

    container = soup.select_one(container_selector)
    if not container:
        logger.warning(f"Container {container_selector} not found")
        return []

    for li in container.select(item_selector):
        item = li.select_one("div.item-product")
        if not item:
            continue

        # Nome + URL
        name_tag = item.select_one(".name a.product-name")
        name = name_tag.get_text(strip=True) if name_tag else None

        raw_url = name_tag["href"] if name_tag and name_tag.has_attr("href") else None
        url = urljoin(BASE_URL, raw_url) if raw_url else None

        # Autor
        author_tag = item.select_one("p.author a")
        author_name = author_tag.get_text(strip=True) if author_tag else None

        author_id = None
        if author_tag and "author_id=" in author_tag.get("href", ""):
            author_id = author_tag["href"].split("author_id=")[-1]

        # Preços
        old_tag = item.select_one(".price .price-old")
        new_tag = item.select_one(".price .price-new")

        price_old = old_tag.get_text(strip=True) if old_tag else None
        price_new = new_tag.get_text(strip=True) if new_tag else None

        # Desconto
        discount_tag = item.select_one(".flag-discount")
        discount = discount_tag.get_text(strip=True) if discount_tag else None

        # Flags
        is_new = bool(item.select_one(".flag.novidade"))

        date_scrap = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        product = {
            "name": name,
            "url": url,
            "author_name": author_name,
            "author_id": author_id,
            "price_old": price_old,
            "price_new": price_new,
            "discount": discount,
            "is_new": is_new,
            "source": source,
            "created_at": date_scrap,
        }

        if has_category:
            product["category"] = category_name

        products.append(product)

    logger.info(f"[{source}] {len(products)} produto(s).")
    return products


def parse_home_sales(response) -> list:
    """Parsea a lista de livros da home page"""
    return parse_products_page(
        response,
        container_selector="div.box.product_featured",
        item_selector="li.product",
        source="home_featured",
        has_category=False,
    )


def parse_content_pages(response) -> list:
    """Parsea a lista de livros de categoria"""
    return parse_products_page(
        response,
        container_selector="div.product-list",
        item_selector="div.product",
        source="category_page",
        has_category=True,
    )


# ------------------------------ livros_em_destaque ------------------------------


def _http() -> HttpClient:
    return HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)


def extract_livros_em_destaque(cfg: PipelineConfig) -> None:
    http = _http()
    html = http.get_text(cfg.url_base)
    if html is None:
        return
    http.save_json(parse_home_sales(html), cfg.landing_dir, cfg.landing_file)


def transform_livros_em_destaque(cfg: PipelineConfig) -> None:
    frames = []
    for f in sorted(cfg.landing_dir.glob(cfg.options["file_pattern"])):
        df = pd.read_json(f)
        df["source_filename"] = f.name
        frames.append(df)
    write_bronze(cfg, pd.concat(frames, ignore_index=True) if frames else None)


# ------------------------------ categorias ------------------------------


def extract_categorias(cfg: PipelineConfig) -> None:
    """Todas as páginas de cada categoria de ``options.hrefs``."""
    http = _http()
    delay = float(cfg.options.get("delay", 1.0))

    for item in cfg.options["hrefs"]:
        name, link = item["name"], item["link"]
        sep = "&" if "?" in link else "?"
        html = http.get_text(f"{link}{sep}page=1")
        if html is None:
            continue
        last_page = get_last_page_number(html)
        logger.info(f"{name}: {last_page} pagina(s)")
        http.save_json(
            parse_content_pages(html),
            cfg.landing_dir,
            cfg.landing_file.format(name=name, page=1),
        )
        for page in range(2, last_page + 1):
            sleep(delay)
            html = http.get_text(f"{link}{sep}page={page}")
            if html is None:
                continue
            http.save_json(
                parse_content_pages(html),
                cfg.landing_dir,
                cfg.landing_file.format(name=name, page=page),
            )


ETLS = {
    "livros_em_destaque": Etl(
        extract=extract_livros_em_destaque, transform=transform_livros_em_destaque
    ),
    "categorias": Etl(extract=extract_categorias),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.livros.vide_editorial.vide_editorial_etl [entidade ...]
