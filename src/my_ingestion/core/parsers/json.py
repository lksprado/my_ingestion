"""Parsers de JSON para DataFrame (ex-json_parsers.py de demodados)."""

import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def make_df_from_json_list(filepath: str, list_key: str) -> pd.DataFrame:
    """Cria DataFrame a partir de uma lista sob ``list_key`` num arquivo JSON."""
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    if list_key not in data:
        logger.warning(f"Chave '{list_key}' não encontrada no JSON.")
        return pd.DataFrame()

    try:
        return pd.DataFrame(data[list_key])
    except (ValueError, TypeError) as e:
        logger.error(f"Erro ao criar DataFrame: {e}")
        return pd.DataFrame()


def normalize_json_object(filepath: str, key: str = None) -> pd.DataFrame:
    """Achata um objeto JSON (opcionalmente sob ``key``) com ``pd.json_normalize``."""
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    if key:
        data = data.get(key, {})

    try:
        return pd.json_normalize(data, sep=".")
    except Exception as e:
        logger.error(f"Erro ao normalizar JSON: {e}")
        return pd.DataFrame()
