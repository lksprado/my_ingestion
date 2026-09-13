"""Parser do day_summary do OpenWeather (Celsius, tipos consistentes)."""

import pandas as pd

_TEMPERATURE_COLS = [
    "temperature_min",
    "temperature_max",
    "temperature_afternoon",
    "temperature_night",
    "temperature_evening",
    "temperature_morning",
]
_INT_COLS = [
    "cloud_cover_afternoon",
    "humidity_afternoon",
    "precipitation_total",
    "wind_max_direction",
    "pressure_afternoon",
]
_DROP_COLS = ["tz", "units"]


def parse_day_summary(content: dict) -> pd.DataFrame:
    """Achata um day_summary (uma linha) e converte Kelvin -> Celsius."""
    df = pd.json_normalize(content)
    df.columns = [c.replace(".", "_") for c in df.columns]
    for col in _TEMPERATURE_COLS:
        df[col] = (df[col].astype(float) - 273.15).round(2)
    for col in _INT_COLS:
        df[col] = df[col].astype(int)
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    return df.drop(columns=[c for c in _DROP_COLS if c in df.columns])
