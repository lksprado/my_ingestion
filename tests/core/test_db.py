"""PostgresClient sem Postgres: validação, plano de colunas e serialização.

O que emite SQL (``ensure_raw_table``, ``COPY``) só é testável com conexão real —
``psycopg2.sql`` precisa dela para citar identificadores. Essa parte está em
``test_copy_load.py`` (``@pytest.mark.integration``); aqui fica tudo que é puro.
"""

import pandas as pd
import pytest

from core.db import (
    ColumnPlan,
    PostgresClient,
    plan_columns,
    read_csv_header,
    validate_raw_schema,
    validate_write_mode,
)
from settings import DbTarget


@pytest.mark.parametrize("schema", ["raw_camara", "raw_b3", "raw_x1_y"])
def test_validate_raw_schema_accepts_raw_prefix(schema):
    assert validate_raw_schema(schema) == schema


@pytest.mark.parametrize(
    "schema", [None, "", "raw", "staging", "rawcamara", "raw_x; drop", "Raw_X"]
)
def test_validate_raw_schema_rejects(schema):
    with pytest.raises(ValueError, match="raw_<fonte>"):
        validate_raw_schema(schema)


@pytest.mark.parametrize("write", ["truncate", "append"])
def test_validate_write_mode_accepts(write):
    assert validate_write_mode(write) == write


@pytest.mark.parametrize("write", [None, "", "replace", "upsert", "merge"])
def test_validate_write_mode_rejects(write):
    with pytest.raises(ValueError, match="inválido"):
        validate_write_mode(write)


class _Sentinel:
    """Dublê que levanta em qualquer uso, provando que a validação vem antes."""

    def __getattr__(self, name):
        raise AssertionError(f"não deveria ser usado ({name})")


@pytest.mark.parametrize("write", ["merge", "replace"])
def test_copy_csv_validates_write_mode_before_reading(tmp_path, write):
    pg = PostgresClient(connection=_Sentinel())
    with pytest.raises(ValueError, match="inválido"):
        pg.copy_csv(tmp_path / "nao_existe.csv", "t", schema="raw_x", write=write)


def test_copy_csv_validates_before_reading(tmp_path):
    pg = PostgresClient(connection=_Sentinel())
    with pytest.raises(ValueError, match="raw_<fonte>"):
        pg.copy_csv(tmp_path / "nao_existe.csv", "t", schema="staging")


def test_load_files_validates_before_reading(tmp_path):
    pg = PostgresClient(connection=_Sentinel())
    with pytest.raises(ValueError, match="raw_<fonte>"):
        pg.load_files_to_table(tmp_path, schema="staging")


def test_client_uses_injected_target_without_env():
    target = DbTarget(
        host="h", port=1, name="ingestion_sandbox", user="u", password="p"
    )
    engine = PostgresClient(target=target).alchemy()  # create_engine não conecta
    assert engine.url.database == "ingestion_sandbox"
    assert engine.url.host == "h"


def test_read_sql_prefers_injected_connection(monkeypatch):
    seen = {}

    def fake_read_sql(sql, con):
        seen.update(sql=sql, con=con)
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    conn = object()
    PostgresClient(connection=conn).read_sql("select 1")
    assert seen == {"sql": "select 1", "con": conn}


def test_read_sql_wraps_engine_queries_in_text(monkeypatch):
    seen = {}

    def fake_read_sql(sql, con):
        seen["sql"] = sql
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    PostgresClient(engine=_Sentinel()).read_sql("select 1 where x like 'a%'")
    assert str(seen["sql"]) == "select 1 where x like 'a%'"
    assert not isinstance(seen["sql"], str)  # sqlalchemy.text


# ------------------------ plano de colunas (drift) ------------------------


def test_plan_columns_sem_tabela_usa_tudo_do_dado():
    assert plan_columns(["a", "b"], []) == ColumnPlan(
        copy=["a", "b"], add=["a", "b"], missing=[]
    )


def test_plan_columns_preserva_ordem_do_dado():
    plano = plan_columns(["b", "a"], ["a", "b"])
    assert plano.copy == ["b", "a"]
    assert plano.add == []


def test_plan_columns_detecta_coluna_nova_e_sumida():
    plano = plan_columns(
        ["a", "nova"], ["a", "antiga", "arquivo_origem", "loaded_at_utc"]
    )
    assert plano.add == ["nova"]
    assert plano.missing == ["antiga"]  # rastreio não conta como sumida


# ------------------------ CSV ------------------------


def test_read_csv_header_ignora_bom_e_desaspa(tmp_path):
    path = tmp_path / "x.csv"
    path.write_text('\ufeffa;"b;c";d\n1;2;3\n', encoding="utf-8")
    assert read_csv_header(path, ";") == ["a", "b;c", "d"]


def test_read_csv_header_rejeita_arquivo_vazio(tmp_path):
    path = tmp_path / "x.csv"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="sem cabeçalho"):
        read_csv_header(path, ";")


def test_load_files_manda_csv_direto_para_o_copy(tmp_path, monkeypatch):
    (tmp_path / "acoes.csv").write_text("a;b\n1;2\n")
    pg = PostgresClient(connection=_Sentinel())
    chamadas = []
    monkeypatch.setattr(
        pg,
        "copy_csv",
        lambda path, table, **kw: chamadas.append((path.name, table, kw)),
    )
    pg.load_files_to_table(tmp_path, schema="raw_b3")

    assert chamadas == [
        (
            "acoes.csv",
            "acoes",
            {
                "schema": "raw_b3",
                "sep": ";",
                "write": "truncate",
                "filename": "acoes.csv",
            },
        )
    ]
