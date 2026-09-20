from functools import partial
from pathlib import Path

import pandas as pd
import pytest

from core import PipelineConfig
from pipelines.esportes.nhl import nhl_etl

PARAMS = {
    nhl_etl.params_jogos: ["game_id"],
    nhl_etl.params_times: ["team_id", "season_id", "game_type_id"],
    nhl_etl.params_jogadores: ["player_id"],
    nhl_etl.params_jogadores_temporada: ["player_id", "season_id", "game_type_id"],
}


def _cfg_raw(**kwargs) -> PipelineConfig:
    return PipelineConfig(
        landing_dir=".",
        db_schema="raw_nhl",
        db_table="nhl_raw_all_games_details",
        options={"control_table": "nhl_ingestion_control"},
        criar_dirs=False,
        **kwargs,
    )


class FakePg:
    """``PostgresClient`` que guarda o SQL e devolve um DataFrame fixo."""

    sql: str = ""

    def __init__(self, *a, **k): ...

    def read_sql(self, sql):
        FakePg.sql = sql
        return pd.DataFrame(
            {c: [1] for c in ("game_id", "player_id", "season_id", "game_type_id")}
            | {"team_id": ["TOR"]}
        )


@pytest.fixture
def fake_pg(monkeypatch):
    monkeypatch.setattr(nhl_etl, "PostgresClient", FakePg)
    return FakePg


@pytest.mark.parametrize("fn,columns", PARAMS.items())
def test_params_devolvem_as_colunas_da_url(fake_pg, fn, columns):
    rows = fn(_cfg_raw())
    assert list(rows[0]) == columns


def test_params_consultam_o_schema_e_a_tabela_da_carga(fake_pg):
    nhl_etl.params_jogos(_cfg_raw())
    sql = fake_pg.sql
    assert "raw_nhl.nhl_raw_all_games_summary" in sql
    assert "raw_nhl.nhl_ingestion_control" in sql
    assert "table_name = 'nhl_raw_all_games_details'" in sql
    # nenhuma referência a objeto do dbt
    assert "staging" not in sql and "vw_stg_" not in sql


def test_params_rejeitam_schema_fora_da_raw(fake_pg):
    cfg = PipelineConfig(landing_dir=".", db_schema="staging_nhl", criar_dirs=False)
    with pytest.raises(ValueError):
        nhl_etl.params_jogos(cfg)


def test_extract_groups_by_season_and_formats_templates(monkeypatch, tmp_path):
    calls = []

    class FakeHttp:
        def __init__(self, *a, **k): ...

        def fetch_and_save_many(self, tasks, output_dir, workers=1):
            calls.append((Path(output_dir), tasks, workers))

    rows = [
        {"player_id": 1, "season_id": 2024},
        {"player_id": 2, "season_id": 2025},
        {"player_id": 3, "season_id": 2025},
    ]
    monkeypatch.setattr(nhl_etl, "HttpClient", FakeHttp)

    cfg = PipelineConfig(
        landing_dir=tmp_path,
        url_base="https://api/{player_id}/{season_id}",
        landing_file="{player_id}_{season_id}.json",
        options={"season_subdir": True, "workers": 4},
        criar_dirs=False,
    )
    nhl_etl.extract_dynamic(cfg, params=lambda cfg: rows)

    by_dir = {c[0].name: c for c in calls}
    assert set(by_dir) == {"2024", "2025"}
    assert by_dir["2025"][1] == [
        ("https://api/2/2025", "2_2025.json"),
        ("https://api/3/2025", "3_2025.json"),
    ]
    assert by_dir["2025"][2] == 4


def test_extract_pula_arquivo_ja_no_landing(monkeypatch, tmp_path):
    calls = []

    class FakeHttp:
        def __init__(self, *a, **k): ...

        def fetch_and_save_many(self, tasks, output_dir, workers=1):
            calls.append(tasks)

    monkeypatch.setattr(nhl_etl, "HttpClient", FakeHttp)
    (tmp_path / "raw_1.json").write_text("{}")

    cfg = PipelineConfig(
        landing_dir=tmp_path,
        url_base="https://api/{game_id}",
        landing_file="raw_{game_id}.json",
        options={"skip_existing": True},
        criar_dirs=False,
    )
    nhl_etl.extract_dynamic(cfg, params=lambda cfg: [{"game_id": 1}, {"game_id": 2}])

    assert calls == [[("https://api/2", "raw_2.json")]]


def test_extract_sem_nada_pendente_nao_chama_a_api(monkeypatch, tmp_path):
    def boom(*a, **k):
        raise AssertionError("não deveria instanciar o HttpClient")

    monkeypatch.setattr(nhl_etl, "HttpClient", boom)
    (tmp_path / "raw_1.json").write_text("{}")

    cfg = PipelineConfig(
        landing_dir=tmp_path,
        url_base="https://api/{game_id}",
        landing_file="raw_{game_id}.json",
        options={"skip_existing": True},
        criar_dirs=False,
    )
    nhl_etl.extract_dynamic(cfg, params=lambda cfg: [{"game_id": 1}])


def test_latest_season_files(tmp_path):
    for season in ("20232024", "20242025"):
        d = tmp_path / season
        d.mkdir()
        (d / f"1_{season}_2.json").write_text("{}")
    cfg = PipelineConfig(
        landing_dir=tmp_path,
        landing_file="{player_id}_{season_id}_{game_type_id}.json",
        options={"season_subdir": True, "file_pattern": "*_*_*.json"},
        criar_dirs=False,
    )
    assert [f.name for f in nhl_etl.latest_season_files(cfg)] == ["1_20242025_2.json"]


def test_etls_match_yaml_options():
    """URL com placeholder => extract_dynamic com params_*; season_subdir => load."""
    for source, etl in nhl_etl.ETLS.items():
        cfg = PipelineConfig.from_yaml(nhl_etl.CONFIG_FILE, source, criar_dirs=False)
        dynamic = "{" in cfg.url_base
        if dynamic:
            assert isinstance(etl.extract, partial), source
            assert etl.extract.func is nhl_etl.extract_dynamic, source
            assert etl.extract.keywords["params"] in PARAMS, source
        else:
            assert etl.extract is None, source
        assert etl.transform is None, source
        season = bool(cfg.options.get("season_subdir"))
        assert (etl.load is nhl_etl.load_latest_season) is season, source
        # skip_existing só faz sentido onde a carga é incremental
        if cfg.options.get("skip_existing"):
            assert cfg.options.get("overwrite") is False, source


@pytest.mark.integration
@pytest.mark.parametrize("fn,columns", PARAMS.items())
def test_params_rodam_no_postgres(fn, columns):
    """As quatro consultas são SQL válido contra a raw do ambiente."""
    cfg = PipelineConfig.from_yaml(
        nhl_etl.CONFIG_FILE, "games_details", criar_dirs=False
    )
    rows = fn(cfg)
    assert all(set(r) == set(columns) for r in rows)
