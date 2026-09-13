import pandas as pd

from core.text import (
    normalize_string,
    sanitize_columns,
    sanitize_values,
    strip_newlines,
)


def test_normalize_string():
    assert normalize_string("Ações Ordinárias") == "acoes_ordinarias"
    assert normalize_string("  Renda - Fixa__ ") == "renda_fixa"


def test_sanitize_columns_matches_legacy_behavior():
    df = pd.DataFrame(columns=["Nome Ação", "ultimoStatus.uri", "Valor (R$)", 42])
    out = sanitize_columns(df)
    assert list(out.columns) == ["nome_acao", "ultimostatus_uri", "valor__r__", "42"]
    assert list(df.columns)[0] == "Nome Ação"  # original intacto


def test_sanitize_columns_subset_and_case():
    df = pd.DataFrame(columns=["A b", "C d"])
    assert list(sanitize_columns(df, cols=["A b"], case="upper").columns) == [
        "A_B",
        "C d",
    ]


def test_sanitize_values_skips_numeric_and_excluded():
    df = pd.DataFrame({"nome": ["José, da Silva"], "url": ["http://x/A"], "n": [1]})
    out = sanitize_values(df, exclude=["url"])
    assert out["nome"].tolist() == ["JOSE DA SILVA"]
    assert out["url"].tolist() == ["http://x/A"]
    assert out["n"].tolist() == [1]


def test_strip_newlines_only_text_columns():
    df = pd.DataFrame({"t": ["a\r\nb\nc"], "n": [1]})
    assert strip_newlines(df)["t"].tolist() == ["a b c"]
