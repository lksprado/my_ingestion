"""De-para fuzzy entre `emissor` (renda fixa) e o conglomerado prudencial (FGC).

Le os emissores distintos de `intermediate.int_renda_fixa`, casa cada um por fuzzy
matching contra `Nome da Instituicao` do CSV de conglomerados prudenciais e herda o
`conglomerado`. O resultado permite agrupar posicoes por conglomerado para raciocinar
sobre a cobertura do FGC.

Saida: `de_para_instituicoes_fgc.csv` em SEEDS_ROOT (seed do dbt, sem carga em
banco) — por isso e uma excecao documentada ao GenericETL. Roda standalone, apos
os ETLs de ingestao e a materializacao da camada `intermediate`.
"""

import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

from core import (
    PipelineConfig,
    PostgresClient,
    normalize_string,
    setup_logger,
    write_csv,
)
from settings import settings

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "investimentos_config.yml"

OUTPUT_NAME = "de_para_instituicoes_fgc.csv"
# Mantem apenas produtos cobertos pelo FGC: o tipo e o primeiro token de
# `investimento` (ex.: 'CDB BANCO ...'). Debentures ('DEB ...') ficam de fora.
_SQL_EMISSORES = (
    "SELECT DISTINCT emissor FROM intermediate.int_renda_fixa "
    "WHERE emissor IS NOT NULL AND emissor <> '' "
    "AND split_part(upper(investimento), ' ', 1) IN ('CDB', 'LCA', 'LCI', 'LC') "
    "ORDER BY emissor"
)


def _normalize_for_match(s: str) -> str:
    """Normaliza um nome apenas para pontuar o fuzzy (nao altera o valor gravado).

    Ascii-fold + upper + remove pontuacao, colapsando espacos. Sufixos societarios
    comuns (S.A., LTDA, etc.) sao removidos por serem ruido compartilhado por quase
    todas as instituicoes, o que enviesaria o token_set_ratio para cima.
    """
    s = (
        unicodedata.normalize("NFKD", str(s))
        .encode("ascii", "ignore")
        .decode("ascii")
        .upper()
    )
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(
        r"\b(S A|SA|LTDA|ME|EPP|BCO|CFI|SOCIEDADE|DE|CREDITO|FINANCIAMENTO|"
        r"INVESTIMENTO|MULTIPLO|BANCO)\b",
        " ",
        s,
    )
    return re.sub(r"\s+", " ", s).strip()


def build_depara(csv_path: Path, output_dir: Path, threshold: int = 80) -> Path:
    """Constroi o de-para emissor -> conglomerado e escreve o CSV. Retorna o path."""
    emissores = PostgresClient(log=logger).read_sql(_SQL_EMISSORES)["emissor"].tolist()
    logger.info("Emissores distintos lidos de int_renda_fixa: %d", len(emissores))

    inst = pd.read_csv(csv_path)
    # chave normalizada -> mantem a primeira ocorrencia (nomes oficiais sao unicos)
    inst["_match_key"] = inst["Nome da Instituição"].map(_normalize_for_match)
    choices = {i: key for i, key in inst["_match_key"].items()}

    rows = []
    for emissor in emissores:
        query = _normalize_for_match(emissor)
        best = process.extractOne(query, choices, scorer=fuzz.token_set_ratio)
        # best = (matched_key, score, index_no dict de choices)
        idx = best[2]
        score = round(best[1], 1)
        matched = inst.loc[idx]

        if score < threshold:
            logger.warning(
                "Match fraco (score %.1f): %r -> %r [conglomerado: %r]",
                score,
                emissor,
                matched["Nome da Instituição"],
                matched["conglomerado"],
            )

        rows.append(
            {
                "nome_instituicao": emissor,
                "nome_conglomerado": matched["conglomerado"],
                "data_extracao": matched["data_extracao"],
                "nome_instituicao_oficial": matched["Nome da Instituição"],
                "score_match": score,
            }
        )

    df = pd.DataFrame(rows)
    df.columns = df.columns.map(normalize_string)
    return write_csv(df, output_dir, OUTPUT_NAME, sep=",")  # seed do dbt: ","


if __name__ == "__main__":
    setup_logger()
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "fgc")
    build_depara(cfg.landing_filepath, settings.seeds_root)
    # uv run python -m pipelines.financas.investimentos.investimentos_fgc
