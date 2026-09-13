"""JSONs horários do APsystems -> DataFrames diário e horário."""

import logging
import re
from pathlib import Path

import pandas as pd

from core import PipelineConfig

logger = logging.getLogger(__name__)
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_hourly_json(path: Path) -> pd.DataFrame:
    """Um arquivo (um dia): uma linha por hora, com ``date`` vinda do nome."""
    df = pd.read_json(path).reset_index().rename(columns={"index": "hour"})
    df["filename"] = path.name
    df["date"] = _DATE_RE.search(path.name).group(0)
    return df


def load_landing(cfg: PipelineConfig) -> pd.DataFrame:
    """Concatena todos os JSONs do landing (arquivo inválido é pulado)."""
    frames = []
    for f in sorted(cfg.landing_dir.glob(cfg.landing_file.format(day="*"))):
        try:
            frames.append(parse_hourly_json(f))
        except Exception as e:
            logger.warning(f"JSON vazio ou inválido {f} -- {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por dia: duração, total, CO2 e pico."""
    if df.empty:
        return df
    out = df[["date", "duration", "total", "co2", "max"]].drop_duplicates(
        subset=["date"]
    )
    out = out.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out


def hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por data-hora com a energia gerada."""
    if df.empty:
        return df
    out = df.copy()
    out["datetime"] = pd.to_datetime(out["date"]) + pd.to_timedelta(
        out["hour"], unit="h"
    )
    return out[["datetime", "energy"]].drop_duplicates(subset=["datetime"])
