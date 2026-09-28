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


def _sanitize_name(col) -> str:
    name = unidecode(str(col)).strip().lower().replace(" ", "_")
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Devolve uma cópia com nomes de coluna sem acento, minúsculos e só ``[a-z0-9_]``.

    É o que define os nomes das colunas em ``raw_<fonte>.*`` (o dbt depende
    deles); ``write_bronze`` já aplica isto.
    """
    return df.rename(columns={c: _sanitize_name(c) for c in df.columns})


def sanitize_values(df: pd.DataFrame, *, exclude: Sequence[str] = ()) -> pd.DataFrame:
    """Devolve uma cópia com os valores texto sem acento/pontuação e em maiúsculas.

    Colunas numéricas e as listadas em ``exclude`` (URLs, e-mails, datas) são
    preservadas. Nulos continuam nulos (sem virar o texto ``NAN``/``NONE``).
    """
    df = df.copy()
    for col in df.columns:
        if col in exclude or pd.api.types.is_numeric_dtype(df[col]):
            continue
        s = df[col].map(lambda v: unidecode(str(v)).strip(), na_action="ignore")
        s = s.astype("object").str.upper()
        df[col] = s.str.replace(r"[^\w\s]", "", regex=True)
    return df


_NEWLINES = re.compile(r"[\r\n]+")


def strip_newlines(df: pd.DataFrame) -> pd.DataFrame:
    """Troca CR/LF por espaço nas colunas texto (CSV ``;`` não sobrevive a eles).

    Coluna ``str`` é tratada vetorizada, e só nas linhas que têm quebra; coluna
    ``object`` (texto misturado com dict/list) vai valor a valor, e o que não é
    ``str`` fica como está.
    """
    df = df.copy()
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_numeric_dtype(s):
            continue
        if isinstance(s.dtype, pd.StringDtype):
            tem = s.str.contains(_NEWLINES.pattern, regex=True, na=False)
            if tem.any():
                df.loc[tem, col] = s[tem].str.replace(
                    _NEWLINES.pattern, " ", regex=True
                )
            continue
        df[col] = s.map(lambda v: _NEWLINES.sub(" ", v) if isinstance(v, str) else v)
    return df
