#!/usr/bin/env python
"""Rebaixa as votações da Câmara de todos os trimestres desde um ano.

O extract de rotina de `votacoes` só pega os últimos `options.trimestres`
trimestres. Antes de ele pegar o trimestre anterior, cada virada de trimestre
perdia o que foi registrado depois da última execução, e há trimestres com
páginas que falharam. Isso deixou lacunas no histórico. Este script preenche as
lacunas uma vez, sobrescrevendo os arquivos `votacoes_<ano>_<Q>_<p>.json` do
landing:

    uv run python scripts/camara_votacoes_backfill.py --desde 2001

Rode onde está o landing que alimenta a raw (em prod, o container do Airflow).
Depois, dispare a DAG da Câmara: o transform/load de `votacoes` reconstrói o
bronze com o landing inteiro, e as entidades por ID buscam só os IDs novos.
"""

import argparse
import sys

from core import PipelineConfig, setup_logger
from pipelines.legislativo.camara.camara_etl import CONFIG_FILE, extract_votacoes


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--desde", type=int, default=2001, help="primeiro ano (default: 2001)"
    )
    args = parser.parse_args(argv)

    setup_logger()
    extract_votacoes(PipelineConfig.from_yaml(CONFIG_FILE, "votacoes"), args.desde)
    return 0


if __name__ == "__main__":
    sys.exit(main())
