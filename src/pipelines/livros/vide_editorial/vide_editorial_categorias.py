"""Páginas de categoria da Vide Editorial (extract-only; ``options.hrefs``)."""

import logging
from pathlib import Path
from time import sleep

from core import GenericETL, HttpClient, PipelineConfig, run_cli
from pipelines.livros.vide_editorial._parsers import (
    get_last_page_number,
    parse_content_pages,
)

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "vide_editorial_config.yml"


def extract(cfg: PipelineConfig) -> None:
    http = HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
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


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "categorias")
    return GenericETL(cfg, extract_fn=extract, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.livros.vide_editorial.vide_editorial_categorias
