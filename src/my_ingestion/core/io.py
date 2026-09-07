"""Helpers de arquivo: listagem, concat e escrita de CSV (era duplicado em 5 lugares)."""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def list_files(input_dir: Path | str, pattern: str = "*") -> list[Path]:
    """Lista arquivos recursivamente, ordenados e resolvidos."""
    input_dir = Path(input_dir)
    return sorted(f.resolve() for f in input_dir.rglob(pattern) if f.is_file())


def concat_files_to_df(
    input_dir: Path | str,
    pattern: str = "*.csv",
    sep: str = ",",
    source_column: str | None = None,
    dedup_key: str | list[str] | None = None,
    existing: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Concatena CSV/JSON de um diretório num único DataFrame.

    Args:
        source_column: se definido, adiciona coluna com o nome do arquivo de origem.
        dedup_key: coluna(s) para deduplicar mantendo a última ocorrência —
            cobre consolidações idempotentes (ex.: extraction_month).
        existing: DataFrame já consolidado a que os novos dados são anexados
            antes do dedup.
    """
    input_dir = Path(input_dir)
    dfs = [] if existing is None else [existing]
    files = sorted(input_dir.glob(pattern))
    if not files and existing is None:
        logger.warning(f"⚠️ Nenhum arquivo ({pattern}) em {input_dir}")
        return pd.DataFrame()

    for file in files:
        if file.suffix.lower() == ".json":
            df = pd.read_json(file, encoding="utf-8")
        else:
            df = pd.read_csv(file, sep=sep, encoding="utf-8", low_memory=False)
        if source_column:
            df[source_column] = file.name
        dfs.append(df)

    result = pd.concat(dfs, ignore_index=True)
    if dedup_key is not None:
        result = result.drop_duplicates(subset=dedup_key, keep="last").reset_index(
            drop=True
        )
    return result


def write_csv(
    df: pd.DataFrame, output_dir: Path | str, filename: str, sep: str = ";"
) -> Path:
    """Grava um DataFrame em CSV, criando o diretório se preciso."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if not filename.endswith(".csv"):
        filename = f"{filename}.csv"
    filepath = output_dir / filename
    df.to_csv(filepath, sep=sep, index=False)
    logger.info(f"💾 CSV salvo em: {filepath}")
    return filepath
