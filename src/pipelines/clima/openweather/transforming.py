"""JSONs do day_summary -> CSV consolidado (Celsius, tipos consistentes)."""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

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


def parsing_daily_weather(
    staging_dir: Path | str, output_name: str = "all_dfs.csv"
) -> str:
    """Consolida todos os JSONs de ``staging_dir`` em ``staging_dir/<output_name>``."""
    logger.info("Iniciando parser...")
    staging_dir = Path(staging_dir)
    frames = []
    for file in sorted(staging_dir.glob("*.json")):
        try:
            frames.append(parse_day_summary(pd.read_json(file, typ="series").to_dict()))
        except Exception as e:
            logger.warning(f"Json vazio ou inválido {file} -- {e}")

    if not frames:
        raise FileNotFoundError(f"Nenhum JSON válido em {staging_dir}")

    all_dfs = pd.concat(frames, ignore_index=True)
    file_dest = staging_dir / output_name
    all_dfs.to_csv(file_dest, index=False)
    logger.info(f"Dados consolidados em: {file_dest}")
    return str(file_dest.absolute())
