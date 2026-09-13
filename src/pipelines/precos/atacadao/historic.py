"""Consolida os CSVs mensais de inflação num seed do dbt (repo the_dw).

Entrada: ``${LAKE_ROOT}/bronze/inflation/months`` — saída: ``${SEEDS_ROOT}``.
"""

import logging
from pathlib import Path

from core import concat_files_to_df, setup_logger, write_csv
from settings import settings

logger = logging.getLogger(__name__)


def make_file(input_dir: Path | str, output_dir: Path | str, filename: str) -> None:
    df = concat_files_to_df(input_dir, pattern="*.csv", sep=",")
    if df.empty:
        raise ValueError("Nenhum arquivo CSV encontrado no diretório de entrada.")
    write_csv(df, output_dir, filename, sep=",")


if __name__ == "__main__":
    setup_logger()
    make_file(
        input_dir=settings.lake_root / "bronze" / "inflation" / "months",
        output_dir=settings.seeds_root,
        filename="minha_inflacao",
    )
    # uv run python -m pipelines.precos.atacadao.historic
