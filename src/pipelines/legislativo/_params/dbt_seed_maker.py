"""Tabelas de domínio do Senado como seeds do dbt (demodadosdw).

Exceção consciente: grava em caminho absoluto porque o destino é o repo do dbt
legislativo, que não é o ``SEEDS_ROOT`` (esse aponta para my_analytics).
"""

import logging
from pathlib import Path

import pandas as pd

from core import HttpClient, setup_logger

logger = logging.getLogger(__name__)
SEEDS_DIR = Path("/home/lucas/workspace/demodados/demodadosdw/seeds")
SEEDS = {
    "raw_senado_tipos_entes": "https://legis.senado.leg.br/dadosabertos/processo/entes",
    "raw_senado_tipos_decisao": "https://legis.senado.leg.br/dadosabertos/processo/tipos-decisao",
    "raw_senado_tipos_projetos": "https://legis.senado.leg.br/dadosabertos/processo/siglas",
}


def gerar_seed(nome: str) -> None:
    data = HttpClient(logger).get_json(SEEDS[nome])
    if data is None:
        raise RuntimeError(f"Falha ao obter {nome}.")
    destino = SEEDS_DIR / f"{nome}.csv"
    pd.DataFrame(data).to_csv(destino, sep=",", index=False)  # seed do dbt: ","
    logger.info(f"📄 Seed gravada em {destino}")


if __name__ == "__main__":
    import sys

    setup_logger()
    for nome in sys.argv[1:] or SEEDS:
        gerar_seed(nome)
    # uv run python -m pipelines.legislativo._params.dbt_seed_maker [nome ...]
