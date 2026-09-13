"""PostgresClient sem Postgres: validação de schema, perfil injetado e DDL."""

from contextlib import contextmanager

import pandas as pd
import pytest

from core.db import PostgresClient, validate_raw_schema
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
    target = DbTarget(host="h", port=1, name="analytics_dev", user="u", password="p")
    engine = PostgresClient(target=target).alchemy()  # create_engine não conecta
    assert engine.url.database == "analytics_dev"
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
