"""De-para fuzzy entre `emissor` (renda fixa) e o conglomerado prudencial (FGC).

Le os emissores distintos de `intermediate.int_renda_fixa`, casa cada um por fuzzy
matching contra `Nome da Instituicao` do CSV de conglomerados prudenciais e herda o
`conglomerado`. O resultado permite agrupar posicoes por conglomerado para raciocinar
sobre a cobertura do FGC.

Saida: `de_para_instituicoes_fgc.csv`, carregado (full-refresh) em
`raw.de_para_instituicoes_fgc`. Roda standalone, apos os ETLs de ingestao e a
materializacao da camada `intermediate`.
"""

import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine, text

from my_ingestion.core.text import normalize_string

logger = logging.getLogger(__name__)

OUTPUT_NAME = "de_para_instituicoes_fgc.csv"


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


def build_depara(
    db_url: str,
    csv_path: Path,
    output_dir: Path,
    threshold: int = 80,
) -> str:
    """Constroi o de-para emissor -> conglomerado e escreve o CSV. Retorna o path."""
    csv_path = Path(csv_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            # Mantem apenas produtos cobertos pelo FGC: o tipo e o primeiro token
            # de `investimento` (ex.: 'CDB BANCO ...'). Debentures ('DEB ...')
            # e demais tipos ficam de fora.
            result = conn.execute(
                text(
                    "SELECT DISTINCT emissor "
                    "FROM intermediate.int_renda_fixa "
                    "WHERE emissor IS NOT NULL AND emissor <> '' "
                    "AND split_part(upper(investimento), ' ', 1) "
                    "IN ('CDB', 'LCA', 'LCI', 'LC') "
                    "ORDER BY emissor"
                )
            )
            emissores = [row[0] for row in result]
    finally:
        engine.dispose()

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

    output_path = output_dir / OUTPUT_NAME
    df.to_csv(output_path, index=False)
    logger.info("De-para escrito: %s (%d linhas)", output_path, len(df))
    return str(output_path)
