"""Parsers de JSON para DataFrame."""

import json
import logging
from collections.abc import Sequence
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


def flatten_children(
    records: Sequence[dict], parent_cols: Sequence[str], child_key: str
) -> list[dict]:
    """Uma linha por filho, com as colunas do pai repetidas (``{**pai, **filho}``)."""
    rows = []
    for rec in records:
        parent = {k: rec.get(k) for k in parent_cols}
        for child in rec.get(child_key) or []:
            rows.append({**parent, **child})
    return rows
