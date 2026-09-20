"""PostgresClient sem Postgres: validação de schema, perfil injetado e DDL."""

from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB

from core.db import PostgresClient, to_raw_frame, validate_raw_schema
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


class _Sentinel:
    """Engine falsa: qualquer uso levanta, provando que a validação vem antes."""

    def __getattr__(self, name):
        raise AssertionError(f"engine não deveria ser usada ({name})")


def test_send_df_validates_before_touching_engine():
    pg = PostgresClient(engine=_Sentinel())
    with pytest.raises(ValueError, match="raw_<fonte>"):
        pg.send_df_to_db(pd.DataFrame({"a": [1]}), "t", schema="raw")


def test_load_files_validates_before_reading(tmp_path):
    pg = PostgresClient(engine=_Sentinel())
    with pytest.raises(ValueError, match="raw_<fonte>"):
        pg.load_files_to_table(tmp_path, schema="staging")


def test_client_uses_injected_target_without_env():
    target = DbTarget(
        host="h", port=1, name="ingestion_sandbox", user="u", password="p"
    )
    engine = PostgresClient(target=target).alchemy()  # create_engine não conecta
    assert engine.url.database == "ingestion_sandbox"
    assert engine.url.host == "h"


def test_ensure_schema_runs_ddl_in_transaction():
    executed = []

    class FakeConn:
        def execute(self, stmt):
            executed.append(str(stmt))

    class FakeEngine:
        @contextmanager
        def begin(self):
            yield FakeConn()

    PostgresClient(engine=FakeEngine())._ensure_schema("raw_x")
    assert executed == ["CREATE SCHEMA IF NOT EXISTS raw_x"]


def test_read_sql_prefers_injected_connection(monkeypatch):
    seen = {}

    def fake_read_sql(sql, con):
        seen.update(sql=sql, con=con)
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    conn = object()
    PostgresClient(connection=conn).read_sql("select 1")
    assert seen == {"sql": "select 1", "con": conn}


def test_load_files_reads_semicolon_by_default(tmp_path, monkeypatch):
    (tmp_path / "acoes.csv").write_text("a;b\n1;2\n")
    sent = []
    pg = PostgresClient(engine=_Sentinel())
    monkeypatch.setattr(
        pg, "send_df_to_db", lambda df, t, **kw: sent.append((t, list(df.columns)))
    )
    pg.load_files_to_table(tmp_path, schema="raw_b3")
    assert sent == [("acoes", ["a", "b"])]


def test_read_sql_wraps_engine_queries_in_text(monkeypatch):
    seen = {}

    def fake_read_sql(sql, con):
        seen["sql"] = sql
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    PostgresClient(engine=_Sentinel()).read_sql("select 1 where x like 'a%'")
    assert str(seen["sql"]) == "select 1 where x like 'a%'"
    assert not isinstance(seen["sql"], str)  # sqlalchemy.text


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
    out, dtype = to_raw_frame(df)

    assert dtype == {c: Text for c in df.columns if c != "j"} | {"j": JSONB}
    assert out["s"].tolist() == ["007", None, "x"]
    assert out["i"].tolist() == ["1", "2", "3"]
    assert out["f"].tolist() == ["1.5", None, "2.0"]
    assert out["b"].tolist() == ["True", "False", None]
    assert out["d"].tolist() == ["2026-01-01 00:00:00", None, "2026-01-03 00:00:00"]
    assert out["j"].tolist() == [{"a": 1}, None, [1, 2]]
    assert out["mix"].tolist() == ['{"a": 1}', "x", None]
    assert out["vazia"].tolist() == [None, None, None]


def test_send_df_to_db_writes_text_json_and_timestamp_metadata(monkeypatch):
    captured = {}

    def fake_to_sql(self, name, con, schema, if_exists, index, dtype):
        captured.update(frame=self.copy(), dtype=dtype, name=name, schema=schema)

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)
    pg = PostgresClient(engine=object())
    monkeypatch.setattr(pg, "_ensure_schema", lambda schema: None)

    df = pd.DataFrame({"n": [1], "j": [{"k": "v"}]})
    pg.send_df_to_db(df, "t", schema="raw_x", filename="f.csv")

    assert captured["dtype"] == {
        "n": Text,
        "j": JSONB,
        "arquivo_origem": Text,
        "data_carga": DateTime,
    }
    assert captured["frame"]["n"].tolist() == ["1"]
    assert list(df.columns) == ["n", "j"]  # DataFrame do chamador intacto
