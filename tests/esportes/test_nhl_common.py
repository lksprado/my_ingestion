from pathlib import Path

from core import PipelineConfig
from pipelines.esportes.nhl import _common


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
    monkeypatch.setattr(_common, "HttpClient", FakeHttp)
    monkeypatch.setattr(_common, "fetch_params", lambda cfg: rows)

    cfg = PipelineConfig(
        landing_dir=tmp_path,
        url_base="https://api/{player_id}/{season_id}",
        landing_file="{player_id}_{season_id}.json",
        options={"season_subdir": True, "workers": 4},
        criar_dirs=False,
    )
    _common.extract_dynamic(cfg)

    by_dir = {c[0].name: c for c in calls}
    assert set(by_dir) == {"2024", "2025"}
    assert by_dir["2025"][1] == [
        ("https://api/2/2025", "2_2025.json"),
        ("https://api/3/2025", "3_2025.json"),
    ]
    assert by_dir["2025"][2] == 4


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
    assert [f.name for f in _common.latest_season_files(cfg)] == ["1_20242025_2.json"]


def test_build_picks_extract_and_load_by_options(tmp_path, monkeypatch):
    def fake_from_yaml(cls, config_file, source, **kw):
        opts = {
            "static": {},
            "dyn": {"param_view": "v"},
            "log": {"param_view": "v", "season_subdir": True},
        }
        return PipelineConfig(
            landing_dir=tmp_path, options=opts[source], criar_dirs=False
        )

    monkeypatch.setattr(PipelineConfig, "from_yaml", classmethod(fake_from_yaml))
    assert _common.build("static").extract_fn is None
    assert _common.build("dyn").extract_fn is _common.extract_dynamic
    assert _common.build("dyn").load_fn is None
    assert _common.build("log").load_fn is _common.load_latest_season
