"""Normalização de strings e limpeza de nomes/valores de DataFrames."""

import re
import unicodedata
from collections.abc import Sequence

import pandas as pd
from unidecode import unidecode


def normalize_string(s: str) -> str:
    """ASCII-fold + lowercase + colapsa espaços/hifens/underscores em ``_``."""
    s = (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    s = re.sub(r"[\s_-]+", "_", s)
    return s.strip("_")


def _sanitize_name(col, case: str, space: str, alfanum: str) -> str:
    new_col = unidecode(str(col)).strip()
    if case == "upper":
        new_col = new_col.upper()
    elif case == "lower":
        new_col = new_col.lower()
    if space == "replace":
        new_col = new_col.replace(" ", "_")
    if alfanum == "remove":
        new_col = "".join(c for c in new_col if c.isalnum() or c in "_ ")
    elif alfanum == "replace":
        new_col = "".join(c if c.isalnum() or c in "_ " else "_" for c in new_col)
    return new_col


def sanitize_columns(
    df: pd.DataFrame,
    cols: Sequence[str] | None = None,
    *,
    case: str = "lower",
    space: str = "replace",
    alfanum: str = "replace",
) -> pd.DataFrame:
    """Devolve uma cópia com nomes de coluna sem acento, minúsculos e só ``[a-z0-9_]``.

    É o que define os nomes das colunas em ``raw_<fonte>.*`` (o dbt depende
    deles); ``write_bronze`` já aplica isto.
    """
    cols = list(cols) if cols else list(df.columns)
    mapping = {c: _sanitize_name(c, case, space, alfanum) for c in cols}
    return df.rename(columns=mapping)


def sanitize_values(
    df: pd.DataFrame,
    *,
    exclude: Sequence[str] = (),
    case: str = "upper",
    space: str = "keep",
    alfanum: str = "remove",
) -> pd.DataFrame:
    """Devolve uma cópia com os valores texto sem acento/pontuação e em maiúsculas.

    Colunas numéricas e as listadas em ``exclude`` (URLs, e-mails, datas) são
    preservadas. Nulos continuam nulos (sem virar o texto ``NAN``/``NONE``).
    """
    df = df.copy()
    for col in df.columns:
        if col in exclude or pd.api.types.is_numeric_dtype(df[col]):
            continue
        s = df[col].map(lambda v: unidecode(str(v)).strip(), na_action="ignore")
        s = s.astype("object")
        if case == "upper":
            s = s.str.upper()
        elif case == "lower":
            s = s.str.lower()
        if space == "replace":
            s = s.str.replace(" ", "_", regex=False)
        if alfanum == "remove":
            s = s.str.replace(r"[^\w\s]", "", regex=True)
        df[col] = s
    return df


def strip_newlines(df: pd.DataFrame) -> pd.DataFrame:
    """Troca CR/LF por espaço nas colunas texto (CSV ``;`` não sobrevive a eles)."""
    df = df.copy()
    pattern = re.compile(r"[\r\n]+")
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        df[col] = df[col].map(
            lambda v: pattern.sub(" ", v) if isinstance(v, str) else v
        )
    return df
