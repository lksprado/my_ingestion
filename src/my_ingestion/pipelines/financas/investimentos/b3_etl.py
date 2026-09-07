import logging
import warnings
from collections import defaultdict
from pathlib import Path

import pandas as pd

from my_ingestion.core.io import list_files
from my_ingestion.core.text import normalize_string

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 50)

logger = logging.getLogger(__name__)

# Arquivos com nomes diferentes mas mesmo schema, consolidados sob um nome só.
FILE_ALIASES = {"proventos_recebidos.csv": "proventos.csv"}


def _list_sheets(file: Path) -> list:
    xl = pd.ExcelFile(file, engine="openpyxl")
    return xl.sheet_names


def make_df(file: Path, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(file, sheet_name=sheet, engine="openpyxl")
    df = df.dropna(subset=[df.columns[0]])
    return df


def run_consolidation(input_dir: Path, output_dir: Path):
    """Lê cada relatório Excel e escreve um consolidado por categoria de ativo.

    Explode as abas de todas as planilhas em memória, agrupa pelo nome normalizado
    da aba (aplicando FILE_ALIASES) e concatena, sem passar por CSVs intermediários.
    `source_path` guarda o caminho absoluto do Excel de origem (que inclui a pessoa).
    """
    input_dir = Path(input_dir).resolve()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_list = list_files(input_dir)
    logger.info("Consolidando %d planilha(s) de %s", len(file_list), input_dir)
    if not file_list:
        logger.warning("Nenhuma planilha encontrada em %s", input_dir)

    grouped: dict[str, list[pd.DataFrame]] = defaultdict(list)
    for f in file_list:
        sheets = _list_sheets(f)
        logger.debug("%s -> %d aba(s)", f.name, len(sheets))
        for s in sheets:
            name = normalize_string(str(s)).removeprefix("posicao_") + ".csv"
            target = FILE_ALIASES.get(name, name)
            if target != name:
                logger.debug("Alias: %s -> %s", name, target)
            df = make_df(f, s)
            df.columns = df.columns.map(normalize_string)
            df["source_path"] = str(f)
            grouped[target].append(df)

    for file_name, dfs in grouped.items():
        df_consolidated = pd.concat(dfs, ignore_index=True)
        output_path = output_dir / ("consolidado_" + file_name)
        df_consolidated.to_csv(output_path, index=False)
        logger.info(
            "Consolidado %s: %d aba(s) -> %d linhas, %d colunas",
            file_name,
            len(dfs),
            len(df_consolidated),
            len(df_consolidated.columns),
        )

    logger.info("Consolidação concluída: %d arquivo(s) em %s", len(grouped), output_dir)
