"""Extract/transform comuns do e-Cidadania: HTML -> CSV no landing -> bronze."""

import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from core import HttpClient, PipelineConfig, write_bronze
from core.parsers.html import make_bs_object
from pipelines.legislativo._common import concat_landing

logger = logging.getLogger(__name__)


def fetch_to_csv(
    cfg: PipelineConfig,
    parser: Callable[[BeautifulSoup], pd.DataFrame],
    url: str,
    filename: str,
    http: HttpClient | None = None,
) -> None:
    """Baixa ``url``, parseia e grava o CSV (``;``) em ``landing_dir/filename``."""
    html = (http or HttpClient(logger)).get_text(url)
    if html is None:
        logger.warning(f"⚠️ Sem resposta de {url}")
        return
    df = parser(make_bs_object(response=html))
    df.to_csv(cfg.landing_dir / filename, sep=";", index=False)


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", dtype=str)


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, _read_csv, pattern="*.csv"))
