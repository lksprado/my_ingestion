#!/usr/bin/env python
"""Semeia a tabela de controle com o landing que a tabela raw já contém.

Necessário uma única vez ao ligar `write: append` + `options.control_table` numa
entidade que já tem histórico carregado: sem isso o primeiro bronze-delta traria
todo o landing e o COPY duplicaria a tabela inteira.

    uv run python scripts/controle_semear.py \
        src/pipelines/legislativo/camara/camara_config.yml votos_deputados

Recusa quando já existe registro para a tabela (nada a semear) e quando a
entidade não declara `control_table`. Use --forcar para semear mesmo assim.
"""

import argparse
import sys
from pathlib import Path

from core import PipelineConfig, setup_logger
from core.control import control_for

REPO = Path(__file__).resolve().parent.parent


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", help="caminho do <fonte>_config.yml")
    parser.add_argument("entidade", help="source do YAML")
    parser.add_argument(
        "--padrao", default="*.json", help="glob do landing (default: *.json)"
    )
    parser.add_argument(
        "--forcar",
        action="store_true",
        help="semeia mesmo com registros já existentes para a tabela",
    )
    args = parser.parse_args(argv)

    log = setup_logger()
    cfg = PipelineConfig.from_yaml(args.config, args.entidade, criar_dirs=False)
    controle = control_for(cfg, log)
    if controle is None:
        log.error(
            f"'{args.entidade}' não declara options.control_table; "
            "nada a semear (a entidade não é incremental por arquivo)."
        )
        return 1

    arquivos = sorted(cfg.landing_dir.glob(args.padrao))
    if not arquivos:
        log.error(f"Nenhum arquivo ({args.padrao}) em {cfg.landing_dir}.")
        return 1

    conn = controle.db.connect()
    try:
        with conn.cursor() as cur:
            controle.ensure(cur)
            ja = controle.ingested(cur, cfg.db_table)
            if ja and not args.forcar:
                log.error(
                    f"{controle.schema}.{controle.table} já tem {len(ja)} registro(s) "
                    f"para {cfg.db_table}; use --forcar se é isso mesmo."
                )
                return 1
            novos = [f.name for f in arquivos if f.name not in ja]
            controle.register(cur, cfg.db_table, novos)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if not controle.db.external_connection:
            conn.close()

    log.info(
        f"📒 {len(novos)} arquivo(s) registrado(s) em "
        f"{controle.schema}.{controle.table} para {cfg.db_table}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
