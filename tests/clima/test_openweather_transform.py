import json

import pandas as pd

from pipelines.clima.openweather.transforming import (
    parse_day_summary,
    parsing_daily_weather,
)

SAMPLE = {
    "lat": -23.137,
    "lon": -46.5547861,
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


def test_parsing_daily_weather_consolidates_and_skips_invalid(tmp_path):
    (tmp_path / "day_summary_2021-09-16.json").write_text(json.dumps(SAMPLE))
    other = {**SAMPLE, "date": "2021-09-17"}
    (tmp_path / "day_summary_2021-09-17.json").write_text(json.dumps(other))
    (tmp_path / "day_summary_bad.json").write_text("{")

    out = parsing_daily_weather(tmp_path)
    df = pd.read_csv(out)
    assert len(df) == 2
    assert list(df["date"]) == ["2021-09-16", "2021-09-17"]
