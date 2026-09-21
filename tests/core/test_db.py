"""PostgresClient sem Postgres: validação, plano de colunas e serialização.

O que emite SQL (``ensure_raw_table``, ``COPY``) só é testável com conexão real —
``psycopg2.sql`` precisa dela para citar identificadores. Essa parte está em
``test_copy_load.py`` (``@pytest.mark.integration``); aqui fica tudo que é puro.
"""

import numpy as np
import pandas as pd
import pytest

from core.db import (
    ColumnPlan,
    PostgresClient,
    df_to_csv_buffer,
    plan_columns,
    read_csv_header,
    to_raw_frame,
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


def test_write_invalido_nao_conecta():
    pg = PostgresClient(connection=_Sentinel())
    with pytest.raises(ValueError, match="inválido"):
        pg.send_df_to_db(pd.DataFrame({"a": ["1"]}), "t", schema="raw_x", write="merge")


class _Sentinel:
    """Dublê que levanta em qualquer uso, provando que a validação vem antes."""

    def __getattr__(self, name):
        raise AssertionError(f"não deveria ser usado ({name})")


def test_send_df_validates_before_connecting():
    pg = PostgresClient(connection=_Sentinel())
    with pytest.raises(ValueError, match="raw_<fonte>"):
        pg.send_df_to_db(pd.DataFrame({"a": [1]}), "t", schema="raw")


def test_send_df_validates_write_mode_before_connecting():
    pg = PostgresClient(connection=_Sentinel())
    with pytest.raises(ValueError, match="inválido"):
        pg.send_df_to_db(pd.DataFrame({"a": [1]}), "t", schema="raw_x", write="replace")


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


# ------------------------ serialização para COPY ------------------------


def test_df_to_csv_buffer_distingue_nulo_de_string_vazia():
    df = pd.DataFrame({"a": [None, "", "x"]}, dtype=object)
    df, tipos = to_raw_frame(df)
    # campo vazio SEM aspas = NULL no COPY; "" = string vazia
    assert df_to_csv_buffer(df, tipos).getvalue() == '\n""\n"x"\n'


def test_df_to_csv_buffer_protege_separador_aspas_e_ponto_barra():
    df = pd.DataFrame({"a": ["a;b", 'as"pas', "\\."]}, dtype=object)
    df, tipos = to_raw_frame(df)
    assert df_to_csv_buffer(df, tipos).getvalue() == '"a;b"\n"as""pas"\n"\\."\n'


def test_df_to_csv_buffer_serializa_jsonb_como_texto_compacto():
    df = pd.DataFrame({"j": [{"a": 1}, None]})
    df, tipos = to_raw_frame(df)
    assert tipos == {"j": "JSONB"}
    assert df_to_csv_buffer(df, tipos).getvalue() == '"{""a"":1}"\n\n'


def test_df_to_csv_buffer_mantem_nulo_em_tabela_de_uma_coluna():
    # csv.writer aspa o campo unico vazio; aqui a linha fica em branco, que e
    # como o COPY representa NULL numa tabela de uma coluna so.
    df, tipos = to_raw_frame(pd.DataFrame({"a": [None, ""]}, dtype=object))
    assert df_to_csv_buffer(df, tipos).getvalue() == '\n""\n'


def test_read_csv_header_ignora_bom_e_desaspa(tmp_path):
    path = tmp_path / "x.csv"
    path.write_text('\ufeffa;"b;c";d\n1;2;3\n', encoding="utf-8")
    assert read_csv_header(path, ";") == ["a", "b;c", "d"]


def test_read_csv_header_rejeita_arquivo_vazio(tmp_path):
    path = tmp_path / "x.csv"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="sem cabeçalho"):
        read_csv_header(path, ";")


# ------------------------ tipagem da raw ------------------------


def test_to_raw_frame_everything_text_except_json():
    df = pd.DataFrame(
        {
            "s": ["007", np.nan, "x"],
            "i": [1, 2, 3],
            "f": [1.5, np.nan, 2.0],
            "b": [True, False, None],
            "d": pd.to_datetime(["2026-01-01", None, "2026-01-03"]),
            "j": [{"a": 1}, None, [1, 2]],
            "mix": [{"a": 1}, "x", None],
            "vazia": [None, None, None],
        }
    )
    out, tipos = to_raw_frame(df)

    assert tipos == {c: "TEXT" for c in df.columns if c != "j"} | {"j": "JSONB"}
    assert out["s"].tolist() == ["007", None, "x"]
    assert out["i"].tolist() == ["1", "2", "3"]
    assert out["f"].tolist() == ["1.5", None, "2.0"]
    assert out["b"].tolist() == ["True", "False", None]
    assert out["d"].tolist() == ["2026-01-01 00:00:00", None, "2026-01-03 00:00:00"]
    assert out["j"].tolist() == [{"a": 1}, None, [1, 2]]
    assert out["mix"].tolist() == ['{"a": 1}', "x", None]
    assert out["vazia"].tolist() == [None, None, None]


def test_send_df_to_db_passa_tipos_e_buffer_para_a_carga(monkeypatch):
    captured = {}
    pg = PostgresClient(connection=_Sentinel())

    def fake_load(schema, table, *, tipos, write, filename, copy):
        captured.update(
            schema=schema, table=table, tipos=tipos, write=write, filename=filename
        )

    monkeypatch.setattr(pg, "_load", fake_load)
    df = pd.DataFrame({"n": [1], "j": [{"k": "v"}]})
    pg.send_df_to_db(df, "t", schema="raw_x", filename="f.csv")

    assert captured["tipos"] == {"n": "TEXT", "j": "JSONB"}
    assert captured["write"] == "truncate"
    assert captured["filename"] == "f.csv"
    # colunas de rastreio não entram no stream: vêm de DEFAULT no catálogo
    assert list(df.columns) == ["n", "j"]  # DataFrame do chamador intacto


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
