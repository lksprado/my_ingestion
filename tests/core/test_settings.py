"""Perfis de conexão por ambiente (DB__<ENV>__*) e guard-rail do banco dev."""

import os

import pytest
from pydantic import ValidationError

BASE = {
    "ENV": "dev",
    "LAKE_ROOT": "/tmp/lake",
    "SEEDS_ROOT": "/tmp/seeds",
    "DB__DEV__HOST": "localhost",
    "DB__DEV__PORT": "5435",
    "DB__DEV__NAME": "ingestion_sandbox",
    "DB__DEV__USER": "u",
    "DB__DEV__PASSWORD": "p@ss/word",
}


def _settings(monkeypatch, **override):
    # Limpa o que veio do shell para o teste depender só do que define aqui.
    for key in list(os.environ):
        if key.startswith("DB__") or key == "ENV":
            monkeypatch.delenv(key)
    for key, value in {**BASE, **override}.items():
        monkeypatch.setenv(key, value)
    from settings import Settings

    return Settings(_env_file=None)


def test_db_target_follows_env(monkeypatch):
    s = _settings(monkeypatch)
    assert s.env == "dev"
    assert s.db_target.name == "ingestion_sandbox"
    assert s.db_target.port == 5435
    # Senha com caracteres especiais é escapada na URL.
    url = s.db_target.url
    assert url.startswith("postgresql+psycopg2://u:p%40ss%2Fword@localhost:5435/")
    assert url.endswith("/ingestion_sandbox")


def test_dev_env_requires_ingestion_sandbox(monkeypatch):
    with pytest.raises(ValidationError, match="DB__DEV__NAME=ingestion_sandbox"):
        _settings(monkeypatch, DB__DEV__NAME="analytics_dev")


def test_models_target_in_dev_is_analytics_dev(monkeypatch):
    s = _settings(monkeypatch)
    assert s.models_target.name == "analytics_dev"
    assert s.models_target.host == s.db_target.host
    assert s.models_target.user == s.db_target.user
    assert s.db_target.name == "ingestion_sandbox"


def test_models_target_in_prod_is_db_target(monkeypatch):
    s = _settings(
        monkeypatch,
        ENV="prod",
        DB__PROD__HOST="pg",
        DB__PROD__NAME="analytics_prod",
        DB__PROD__USER="u",
        DB__PROD__PASSWORD="p",
    )
    assert s.models_target == s.db_target


def test_active_profile_must_be_complete(monkeypatch):
    with pytest.raises(ValidationError, match="DB__PROD__HOST"):
        _settings(monkeypatch, ENV="prod")


def test_inactive_profile_may_be_blank(monkeypatch):
    s = _settings(
        monkeypatch,
        DB__PROD__HOST="",
        DB__PROD__PORT="",
        DB__PROD__NAME="",
    )
    assert s.db.prod.host is None
    assert s.db.prod.port == 5432


def test_prod_profile_resolves(monkeypatch):
    s = _settings(
        monkeypatch,
        ENV="prod",
        DB__PROD__HOST="prod-host",
        DB__PROD__NAME="analytics",
        DB__PROD__USER="u",
        DB__PROD__PASSWORD="p",
    )
    assert s.db_target.host == "prod-host"
    assert s.db_target.name == "analytics"


def test_unknown_env_fails(monkeypatch):
    with pytest.raises(ValidationError):
        _settings(monkeypatch, ENV="prod")
