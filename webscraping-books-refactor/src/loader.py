from pathlib import Path
from typing import Literal

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from .utils.log import logger


def load_data(
    dir: Path, file_extension: Literal["json", "csv"], schema: str, table_name: str
):
    load_dotenv()

    import os

    DB_HOST = os.getenv("DB_HOST")  # noqa: N806
    DB_PORT = os.getenv("DB_PORT")  # noqa: N806
    DB_NAME = os.getenv("DB_NAME")  # noqa: N806
    DB_USER = os.getenv("DB_USER")  # noqa: N806
    DB_PW = os.getenv("DB_PW")  # noqa: N806

    # Engine para to_sql
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PW}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Lê todos os CSVs e concatena
    input_dir = Path(dir)

    dfs = []
    for file in input_dir.iterdir():
        if not file.is_file():
            continue

        if file.suffix.lower() != f".{file_extension}":
            continue

        if file_extension == "csv":
            df = pd.read_csv(file, encoding="utf-8")

        elif file_extension == "json":
            df = pd.read_json(file, encoding="utf-8")

        else:
            continue

        df["source_filename"] = file.name
        dfs.append(df)
    if not dfs:
        logger.warning("No files found.")
        return

    df_final = pd.concat(dfs, ignore_index=True)

    # Insere tudo de uma vez
    try:
        df_final.to_sql(
            name=table_name, con=engine, schema=schema, if_exists="replace", index=False
        )
        row_count = len(df_final)
        logger.info(f"Load complete! {row_count} registers.")
    except Exception as e:
        raise Exception(f"Failed to load data to database: {e}")

    engine.dispose()


if __name__ == "__main__":
    ## LOAD NO LEGADO
    filpath = "/media/lucas/Files/2.Projetos/0.mylake/raw/vide/legacy"
    load_data(
        dir=filpath,
        file_extension="csv",
        schema="raw",
        table_name="vide_raw_all_books_legacy",
    )
