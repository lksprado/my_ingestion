"""Controle de arquivos ingeridos: manifesto e seleção de pendentes (sem Postgres)."""

import pandas as pd
import pytest

from core.config import PipelineConfig
from core.control import (
    control_for,
    manifest_path,
    read_manifest,
    write_bronze_incremental,
    write_manifest,
)


def _cfg(tmp_path, **kw):
    kw.setdefault("bronze_dir", tmp_path / "bronze")
    kw.setdefault("bronze_file", "fonte_entidade.csv")
    kw.setdefault("db_schema", "raw_x")
    kw.setdefault("db_table", "t")
    return PipelineConfig(landing_dir=tmp_path / "landing", **kw)


def test_manifesto_fica_ao_lado_do_bronze(tmp_path):
    cfg = _cfg(tmp_path)
    assert manifest_path(cfg).parent == cfg.bronze_dir
    assert manifest_path(cfg).name == "fonte_entidade.manifesto.csv"


def test_manifesto_ida_e_volta(tmp_path):
    cfg = _cfg(tmp_path)
    write_manifest(cfg, [tmp_path / "a.json", tmp_path / "sub" / "b.json"])
    assert read_manifest(cfg) == ["a.json", "b.json"]


def test_manifesto_ausente_e_lista_vazia(tmp_path):
    assert read_manifest(_cfg(tmp_path)) == []


def test_manifesto_vazio_nao_confunde_com_cabecalho(tmp_path):
    cfg = _cfg(tmp_path)
    write_manifest(cfg, [])
    assert manifest_path(cfg).exists()
    assert read_manifest(cfg) == []


def test_sem_control_table_no_yaml_nao_ha_controle(tmp_path):
    assert control_for(_cfg(tmp_path)) is None


def test_bronze_incremental_sem_controle_faz_rebuild_completo(tmp_path):
    """Sem options.control_table a função é o write_bronze_streaming de sempre."""
    cfg = _cfg(tmp_path)
    landing = tmp_path / "landing"
    for nome in ("a", "b"):
        (landing / f"{nome}.json").write_text("{}", encoding="utf-8")
    files = sorted(landing.glob("*.json"))

    path = write_bronze_incremental(cfg, files, lambda f: pd.DataFrame({"x": [f.stem]}))
    assert path == cfg.bronze_filepath
    assert path.read_text().splitlines() == ["x", "a", "b"]
    assert not manifest_path(cfg).exists()  # sem controle, sem manifesto


def test_bronze_incremental_so_com_pendentes(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, options={"control_table": "ingestion_control"})
    landing = tmp_path / "landing"
    for nome in ("a", "b", "c"):
        (landing / f"{nome}.json").write_text("{}", encoding="utf-8")
    files = sorted(landing.glob("*.json"))

    class FakeControle:
        def pending(self, arquivos, table):
            return [f for f in arquivos if f.name != "a.json"]  # 'a' já ingerido

    monkeypatch.setattr(
        "core.control.control_for", lambda cfg, log=None: FakeControle()
    )

    path = write_bronze_incremental(cfg, files, lambda f: pd.DataFrame({"x": [f.stem]}))
    assert path.read_text().splitlines() == ["x", "b", "c"]
    assert read_manifest(cfg) == ["b.json", "c.json"]


def test_bronze_incremental_sem_pendentes_grava_manifesto_vazio(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, options={"control_table": "ingestion_control"})
    (tmp_path / "landing" / "a.json").write_text("{}", encoding="utf-8")

    class FakeControle:
        def pending(self, arquivos, table):
            return []

    monkeypatch.setattr(
        "core.control.control_for", lambda cfg, log=None: FakeControle()
    )

    assert write_bronze_incremental(cfg, [], lambda f: None) is None
    # manifesto vazio é o que faz o load pular em vez de recarregar bronze velho
    assert manifest_path(cfg).exists()
    assert read_manifest(cfg) == []


def test_ingestion_control_exige_schema_raw():
    from core.control import IngestionControl

    with pytest.raises(ValueError, match="raw_<fonte>"):
        IngestionControl(object(), schema="staging")
