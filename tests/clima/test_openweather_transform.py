import json

import pandas as pd

from core import PipelineConfig
from pipelines.clima.openweather.openweather_etl import parse_day_summary, transform

SAMPLE = {
    "lat": -23.5,
    "lon": -46.6,
    "tz": "-03:00",
    "date": "2021-09-16",
    "units": "standard",
    "cloud_cover": {"afternoon": 100.0},
    "humidity": {"afternoon": 62.0},
    "precipitation": {"total": 0.0},
    "temperature": {
        "min": 287.96,
        "max": 293.52,
        "afternoon": 290.32,
        "night": 288.52,
        "evening": 288.66,
        "morning": 289.77,
    },
    "pressure": {"afternoon": 1022.0},
    "wind": {"max": {"speed": 5.55, "direction": 134.0}},
}


def test_parse_day_summary_flattens_and_converts():
    df = parse_day_summary(SAMPLE)
    row = df.iloc[0]

    assert "tz" not in df.columns and "units" not in df.columns
    assert row["temperature_min"] == 14.81
    assert row["temperature_max"] == 20.37
    assert row["wind_max_speed"] == 5.55
    assert row["humidity_afternoon"] == 62
    assert pd.api.types.is_integer_dtype(df["pressure_afternoon"])
    assert row["date"] == "2021-09-16"


def test_transform_consolidates_and_skips_invalid(tmp_path):
    (tmp_path / "day_summary_2021-09-16.json").write_text(json.dumps(SAMPLE))
    (tmp_path / "day_summary_2021-09-17.json").write_text(
        json.dumps({**SAMPLE, "date": "2021-09-17"})
    )
    (tmp_path / "day_summary_bad.json").write_text("{")
    cfg = PipelineConfig(
        landing_dir=tmp_path,
        bronze_dir=tmp_path,
        landing_file="day_summary_{day}.json",
        bronze_file="all_dfs.csv",
        bronze_sep=",",
    )
    transform(cfg)
    df = pd.read_csv(tmp_path / "all_dfs.csv")
    assert list(df["date"]) == ["2021-09-16", "2021-09-17"]
