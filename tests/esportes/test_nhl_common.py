from pathlib import Path

from core import PipelineConfig
from pipelines.esportes.nhl import _common


def test_extract_groups_by_season_and_formats_templates(monkeypatch, tmp_path):
    calls = []

    class FakeHttp:
        def __init__(self, *a, **k): ...

        def fetch_and_save_many(self, tasks, output_dir, workers=1):
            calls.append((Path(output_dir), tasks, workers))

    monkeypatch.setattr(_common, "HttpClient", FakeHttp)

    cfg = PipelineConfig(
        landing_dir=tmp_path,
        url_base="https://api/{player_id}/{season_id}",
        landing_file="{player_id}_{season_id}.json",
        options={"season_subdir": True, "workers": 4},
        criar_dirs=False,
    )
    rows = [
        {"player_id": 1, "season_id": 2024},
        {"player_id": 2, "season_id": 2025},
        {"player_id": 3, "season_id": 2025},
    ]
    _common.extract(cfg, rows, _common.logging.getLogger("t"))

    assert {c[0].name for c in calls} == {"2024", "2025"}
    by_dir = {c[0].name: c for c in calls}
    assert by_dir["2025"][1] == [
        ("https://api/2/2025", "2_2025.json"),
        ("https://api/3/2025", "3_2025.json"),
    ]
    assert by_dir["2025"][2] == 4


def test_landing_files_picks_latest_season(tmp_path):
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
    files = _common._landing_files(cfg)
    assert [f.name for f in files] == ["1_20242025_2.json"]
