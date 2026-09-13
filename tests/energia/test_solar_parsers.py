import json

import pandas as pd

from core import PipelineConfig
from pipelines.energia.solar._parsers import (
    daily_summary,
    hourly,
    load_landing,
    parse_hourly_json,
)

SAMPLE = {
    "energy": {"0": 0.0, "1": 0.5, "2": 1.2},
    "duration": "8h",
    "total": 12.3,
    "co2": 4.5,
    "max": 1.2,
}


def _write(tmp_path, day):
    p = tmp_path / f"hourly24_production_{day}.json"
    p.write_text(json.dumps(SAMPLE))
    return p


def test_parse_hourly_json_reads_date_from_name(tmp_path):
    df = parse_hourly_json(_write(tmp_path, "2026-01-02"))
    assert df["date"].unique().tolist() == ["2026-01-02"]
    assert df["hour"].tolist() == [0, 1, 2]
    assert df["energy"].tolist() == [0.0, 0.5, 1.2]


def test_load_landing_daily_and_hourly(tmp_path):
    _write(tmp_path, "2026-01-02")
    _write(tmp_path, "2026-01-03")
    (tmp_path / "hourly24_production_bad.json").write_text("{")
    cfg = PipelineConfig(
        landing_dir=tmp_path, landing_file="hourly24_production_{day}.json"
    )
    df = load_landing(cfg)
    assert len(df) == 6

    daily = daily_summary(df)
    assert [str(d) for d in daily["date"]] == ["2026-01-02", "2026-01-03"]
    assert daily["total"].tolist() == [12.3, 12.3]

    h = hourly(df)
    assert len(h) == 6
    assert h["datetime"].iloc[1] == pd.Timestamp("2026-01-02 01:00:00")


def test_empty_landing():
    assert daily_summary(pd.DataFrame()).empty
    assert hourly(pd.DataFrame()).empty
