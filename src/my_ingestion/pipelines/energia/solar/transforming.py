import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def parsing_json_to_dataframe(staging_dir: Path) -> str:
    """_summary_

    Args:
        staging_dir (Path): Local dos jsons

    Returns:
        str: Path do arquivo
    """

    logger.info("Iniciando parser...")
    staging_dir = Path(staging_dir)
    data = []
    for file in staging_dir.iterdir():
        if file.suffix == ".json":
            try:
                df = pd.read_json(file)
                df = df.reset_index().rename(columns={"index": "hour"})
                df["filename"] = file.name
                df["date"] = file.name[20:30]
                data.append(df)
            except Exception as e:
                logger.warning(f"Json vazio ou inválido {file} -- {e}")
    all_dfs = pd.concat(data, ignore_index=True)
    file_dest = staging_dir / "all_dfs.csv"
    all_dfs.to_csv(file_dest, index=False)
    logger.info(f"Dados consolidados em: {file_dest}")
    return str(file_dest.absolute())


def make_daily_summary_df(csv_file: Path) -> str:
    """_summary_

    Args:
        csv_file (Path): Path do arquivo

    Returns:
        str: Path do arquivo
    """
    csv_file = Path(csv_file)
    logger.info("Consolidando dados diarios...")
    df = pd.read_csv(csv_file)
    summary_df = df[["date", "duration", "total", "co2", "max"]].drop_duplicates(
        subset=["date"]
    )
    summary_df["date"] = pd.to_datetime(summary_df["date"]).dt.date
    file_dest = csv_file.parent / "daily_energy.csv"
    summary_df.to_csv(file_dest, index=False)
    logger.info(f"Arquivo para carga salvo em: {file_dest.absolute()}")
    return str(file_dest.absolute())


def make_hourly_df(csv_file: Path) -> str:
    """_summary_

    Args:
        csv_file (Path): Path do arquivo

    Returns:
        str: Path do arquivo
    """
    csv_file = Path(csv_file)
    logger.info("Consolidando dados para data-hora...")
    df = pd.read_csv(csv_file)
    df["datetime"] = pd.to_datetime(df["date"]) + pd.to_timedelta(df["hour"], unit="h")
    hourly_df = df[["datetime", "energy"]].drop_duplicates(subset=["datetime"])
    file_dest = csv_file.parent / "hourly_energy.csv"
    hourly_df.to_csv(file_dest, index=False)
    logger.info(f"Arquivo para carga salvo em: {file_dest.absolute()}")
    return str(file_dest.absolute())
