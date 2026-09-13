from pathlib import Path

import pytest

from core.config import PipelineConfig


def test_pipeline_cfg_normalizes_paths_and_derives_bronze_file(tmp_path: Path):
    landing_dir = tmp_path / "landing"
    bronze_dir = tmp_path / "bronze"

    cfg = PipelineConfig(
        landing_dir=landing_dir,
        bronze_dir=bronze_dir,
        landing_file="data.json",
        bronze_file=None,
        criar_dirs=True,
    )

    # Dirs created and normalized to Path
    assert cfg.landing_dir == landing_dir
    assert cfg.bronze_dir == bronze_dir
    assert landing_dir.exists()
    assert bronze_dir.exists()

    # bronze_file is derived from landing_file with .csv
    assert cfg.bronze_file == "data.csv"

    # Property paths combine dir + file
    assert cfg.landing_filepath == landing_dir / "data.json"
    assert cfg.bronze_filepath == bronze_dir / "data.csv"


def test_pipeline_cfg_resolves_date_template(tmp_path: Path):
    from datetime import datetime

    today = datetime.today().strftime("%Y-%m-%d")
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
        landing_file="data_{date}.json",
        bronze_file="data_{date}.csv",
        criar_dirs=True,
    )

    assert cfg.landing_file == f"data_{today}.json"
    assert cfg.bronze_file == f"data_{today}.csv"
    assert cfg.landing_filepath == tmp_path / "landing" / f"data_{today}.json"


def test_pipeline_cfg_derives_bronze_with_date(tmp_path: Path):
    from datetime import datetime

    today = datetime.today().strftime("%Y-%m-%d")
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        landing_file="data_{date}.json",
        criar_dirs=False,
    )

    assert cfg.bronze_file == f"data_{today}.csv"


def test_pipeline_cfg_missing_fields_raise():
    cfg = PipelineConfig(landing_dir="/tmp/ld", criar_dirs=False)

    with pytest.raises(ValueError):
        _ = cfg.landing_filepath

    cfg.landing_file = "x.json"
    # Now landing_filepath works
    assert isinstance(cfg.landing_filepath, Path)

    # bronze not configured
    with pytest.raises(ValueError):
        _ = cfg.bronze_filepath

    cfg.bronze_dir = Path("/tmp/brz")
    cfg.bronze_file = "x.csv"
    assert isinstance(cfg.bronze_filepath, Path)


def test_pipeline_cfg_keeps_unknown_placeholders(tmp_path: Path):
    from datetime import datetime

    today = datetime.today().strftime("%Y-%m-%d")
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        landing_file="raw_{game_id}_{date}.json",
        criar_dirs=False,
    )
    # só {date} é resolvido; {game_id} fica para o pipeline
    assert cfg.landing_file == f"raw_{{game_id}}_{today}.json"
    assert cfg.landing_file.format(game_id=42) == f"raw_42_{today}.json"


def test_load_source_config_passes_options(tmp_path: Path):
    from core.config import load_source_config

    yml = tmp_path / "cfg.yml"
    yml.write_text(
        "environments:\n  local:\n    base_raw: /tmp/x\n"
        "sources:\n  s:\n    base_url: http://a\n    options:\n      array_key: data\n"
        "  t:\n    base_url: http://b\n",
        encoding="utf-8",
    )
    cfg = PipelineConfig(**load_source_config(yml, "s", env="local"), criar_dirs=False)
    assert cfg.options == {"array_key": "data"}

    cfg = PipelineConfig(**load_source_config(yml, "t", env="local"), criar_dirs=False)
    assert cfg.options == {}


def test_load_source_config_propagates_db_schema(tmp_path: Path):
    from core.config import load_source_config

    yml = tmp_path / "cfg.yml"
    yml.write_text(
        "db_schema: raw_fonte\n"
        "environments:\n  local:\n    base_raw: /tmp/x\n"
        "sources:\n  s:\n    db_table: entidade\n"
        "  t:\n    db_table: outra\n    db_schema: raw_override\n",
        encoding="utf-8",
    )
    cfg = PipelineConfig(**load_source_config(yml, "s", env="local"), criar_dirs=False)
    assert (cfg.db_schema, cfg.db_table) == ("raw_fonte", "entidade")

    # O source pode sobrescrever o schema do arquivo.
    cfg = PipelineConfig(**load_source_config(yml, "t", env="local"), criar_dirs=False)
    assert cfg.db_schema == "raw_override"


def test_load_source_config_without_db_schema(tmp_path: Path):
    from core.config import load_source_config

    yml = tmp_path / "cfg.yml"
    yml.write_text(
        "environments:\n  local:\n    base_raw: /tmp/x\n"
        "sources:\n  s:\n    db_table: e\n",
        encoding="utf-8",
    )
    cfg = PipelineConfig(**load_source_config(yml, "s", env="local"), criar_dirs=False)
    assert cfg.db_schema is None
