"""Parsers de JSON para DataFrame."""

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def normalize_json_object(filepath: Path | str, key: str | None = None) -> pd.DataFrame:
    """Achata um objeto JSON (opcionalmente sob ``key``) com ``pd.json_normalize``."""
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    if key:
        data = data.get(key, {})

    try:
        return pd.json_normalize(data, sep=".")
    except Exception as e:
        logger.error(f"Erro ao normalizar JSON {filepath}: {e}")
        return pd.DataFrame()
