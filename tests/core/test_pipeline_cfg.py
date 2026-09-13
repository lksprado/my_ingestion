from datetime import datetime
from pathlib import Path

import pytest

from core.config import PipelineConfig


def test_pipeline_cfg_normalizes_paths_and_derives_bronze_file(tmp_path: Path):
    landing_dir = tmp_path / "landing"
    bronze_dir = tmp_path / "bronze"

    cfg = PipelineConfig(
        landing_dir=landing_dir, bronze_dir=bronze_dir, landing_file="data.json"
    )

    assert cfg.landing_dir == landing_dir
    assert cfg.bronze_dir == bronze_dir
    assert landing_dir.exists() and bronze_dir.exists()
    assert cfg.bronze_file == "data.csv"
    assert cfg.landing_filepath == landing_dir / "data.json"
    assert cfg.bronze_filepath == bronze_dir / "data.csv"


def test_pipeline_cfg_resolves_date_template(tmp_path: Path):
    today = datetime.today().strftime("%Y-%m-%d")
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        landing_file="data_{date}.json",
        bronze_file="data_{date}.csv",
        criar_dirs=False,
    )
    assert cfg.landing_file == f"data_{today}.json"
    assert cfg.bronze_file == f"data_{today}.csv"


def test_pipeline_cfg_derives_bronze_with_date(tmp_path: Path):
    today = datetime.today().strftime("%Y-%m-%d")
    cfg = PipelineConfig(
        landing_dir=tmp_path, landing_file="data_{date}.json", criar_dirs=False
    )
    assert cfg.bronze_file == f"data_{today}.csv"


def test_pipeline_cfg_missing_fields_raise():
    cfg = PipelineConfig(landing_dir="/tmp/ld", criar_dirs=False)

    with pytest.raises(ValueError):
        _ = cfg.landing_filepath
    with pytest.raises(ValueError):
        _ = cfg.bronze_filepath
    with pytest.raises(ValueError):
        _ = cfg.parameter_filepath

    cfg.landing_file = "x.json"
    cfg.bronze_dir, cfg.bronze_file = Path("/tmp/brz"), "x.csv"
    cfg.parameter_dir, cfg.parameter_file = Path("/tmp/p"), "ids.csv"
    assert cfg.landing_filepath == Path("/tmp/ld/x.json")
    assert cfg.bronze_filepath == Path("/tmp/brz/x.csv")
    assert cfg.parameter_filepath == Path("/tmp/p/ids.csv")


def test_pipeline_cfg_keeps_unknown_placeholders(tmp_path: Path):
    today = datetime.today().strftime("%Y-%m-%d")
    cfg = PipelineConfig(
        landing_dir=tmp_path, landing_file="raw_{game_id}_{date}.json", criar_dirs=False
    )
    assert cfg.landing_file == f"raw_{{game_id}}_{today}.json"
    assert cfg.landing_file.format(game_id=42) == f"raw_42_{today}.json"


def test_pipeline_cfg_rejects_unknown_load_mode(tmp_path: Path):
    with pytest.raises(ValueError, match="load="):
        PipelineConfig(landing_dir=tmp_path, load="upsert", criar_dirs=False)


def test_write_output_params_exports_unique_values(tmp_path: Path):
    import pandas as pd

    cfg = PipelineConfig(
        landing_dir=tmp_path / "ld",
        parameter_dir=tmp_path / "params",
        output_param_file={"ids.csv": "id", "outros.csv": "nao_existe"},
        criar_dirs=False,
    )
    cfg.write_output_params(pd.DataFrame({"id": [1, 1, 2, None]}))
    assert (tmp_path / "params" / "ids.csv").read_text().split() == ["id", "1.0", "2.0"]
    assert not (tmp_path / "params" / "outros.csv").exists()


def _yml(tmp_path: Path, text: str) -> Path:
    yml = tmp_path / "cfg.yml"
    yml.write_text(text, encoding="utf-8")
    return yml


def test_from_yaml_passes_options(tmp_path: Path):
    yml = _yml(
        tmp_path,
        "environments:\n  local:\n    base_raw: /tmp/x\n"
        "sources:\n  s:\n    base_url: http://a\n    options:\n      array_key: data\n"
        "  t:\n    base_url: http://b\n",
    )
    cfg = PipelineConfig.from_yaml(yml, "s", env="local", criar_dirs=False)
    assert cfg.options == {"array_key": "data"}
    assert cfg.url_base == "http://a"

    cfg = PipelineConfig.from_yaml(yml, "t", env="local", criar_dirs=False)
    assert cfg.options == {}


def test_from_yaml_top_level_defaults_and_source_overrides(tmp_path: Path):
    yml = _yml(
        tmp_path,
        "db_schema: raw_fonte\nload: jsonb\nbronze_sep: ','\n"
        "options:\n  control_table: ctl\n  workers: 1\n"
        "environments:\n  local:\n    base_raw: /tmp/x\n"
        "sources:\n  s:\n    db_table: entidade\n"
        "  t:\n    db_table: outra\n    db_schema: raw_override\n    load: none\n"
        "    bronze_sep: ';'\n    options:\n      workers: 4\n",
    )
    cfg = PipelineConfig.from_yaml(yml, "s", env="local", criar_dirs=False)
    assert (cfg.db_schema, cfg.db_table, cfg.load, cfg.bronze_sep) == (
        "raw_fonte",
        "entidade",
        "jsonb",
        ",",
    )
    assert cfg.options == {"control_table": "ctl", "workers": 1}

    cfg = PipelineConfig.from_yaml(yml, "t", env="local", criar_dirs=False)
    assert (cfg.db_schema, cfg.load, cfg.bronze_sep) == ("raw_override", "none", ";")
    assert cfg.options == {"control_table": "ctl", "workers": 4}


def test_from_yaml_defaults_without_top_level_keys(tmp_path: Path):
    yml = _yml(
        tmp_path,
        "environments:\n  local:\n    base_raw: /tmp/x\n"
        "sources:\n  s:\n    db_table: e\n",
    )
    cfg = PipelineConfig.from_yaml(yml, "s", env="local", criar_dirs=False)
    assert cfg.db_schema is None
    assert (cfg.load, cfg.bronze_sep, cfg.options) == ("table", ";", {})
