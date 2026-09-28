#!/usr/bin/env python
"""Reconstrói uma tabela incremental por arquivo a partir do landing inteiro.

Para quando o que já entrou está errado e o incremental não conserta sozinho —
o caso das colunas que o bronze-delta descartava antes de o cabeçalho passar a
crescer (`raw_senado.raw_senado_processo` perdeu `apelido`, `objetivo`,
`normaGerada`). O transform da própria entidade roda sobre o landing inteiro,
num diretório temporário ao lado do bronze, e numa transação só a tabela é
truncada, recebe esse bronze e a tabela de controle passa a listar exatamente os
arquivos que entraram. Falha em qualquer ponto → rollback, tabela e controle
como estavam. Não rode com a DAG da fonte em execução.

    uv run python scripts/controle_reconstruir.py \
        src/pipelines/legislativo/senado/senado_config.yml processo

Em prod, no scheduler (sem uv; o `src/` já está no PYTHONPATH):

    docker exec $(docker ps -qf name=scheduler) bash -c \
        'cd /usr/local/airflow/include/my_ingestion && python \
        scripts/controle_reconstruir.py \
        src/pipelines/legislativo/senado/senado_config.yml processo'

**Sem --confirmar só simula**: roda o transform e mostra linhas e colunas da
tabela contra as do landing, e as colunas que a tabela ganharia — é o
diagnóstico de se a reconstrução vale a pena. Recusa gravar menos linhas do que
a tabela tem (landing incompleto apagaria histórico) sem --forcar.
"""

import argparse
import csv
import importlib
import logging
import sys
import tempfile
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from unittest import mock

from psycopg2 import sql

from core import PipelineConfig, PostgresClient, setup_logger
from core.control import control_for, manifest_path, read_manifest, write_manifest
from core.db import read_csv_header


def etls_da_fonte(config: Path) -> dict:
    """``ETLS`` do ``<fonte>_etl.py`` da pasta do ``<fonte>_config.yml``."""
    partes = config.resolve().parts
    fonte = config.stem.removesuffix("_config")
    modulo = ".".join((*partes[partes.index("pipelines") : -1], f"{fonte}_etl"))
    return importlib.import_module(modulo).ETLS


class _TudoPendente:
    """Controle do transform: todo o landing é pendente, nada vai ao banco."""

    def __init__(self):
        self.sem_linhas: list[str] = []

    def pending(self, files, table):
        return sorted(Path(f) for f in files)

    def mark_ingested(self, files, table):
        self.sem_linhas.extend(Path(f).name for f in files)


def _contar_linhas(path: Path, sep: str) -> int:
    csv.field_size_limit(2**31 - 1)
    with open(path, encoding="utf-8", newline="") as f:
        return sum(1 for _ in csv.reader(f, delimiter=sep)) - 1


def _tabela(db: PostgresClient, schema: str, table: str) -> tuple[int, list[str]]:
    """``(linhas, colunas)`` da tabela; ``(0, [])`` se ela não existe."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema, table),
            )
            colunas = [r[0] for r in cur.fetchall()]
            if not colunas:
                return 0, []
            cur.execute(
                sql.SQL("SELECT count(*) FROM {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
            return cur.fetchone()[0], colunas
    finally:
        if not db.external_connection:
            conn.close()


def reconstruir(
    cfg: PipelineConfig,
    transform: Callable[[PipelineConfig], object],
    *,
    confirmar: bool = False,
    forcar: bool = False,
    log: logging.Logger,
) -> int:
    controle = control_for(cfg, log)
    if controle is None or cfg.write != "append" or cfg.load != "table":
        log.error(
            "A entidade não é incremental por arquivo (write: append, load: table "
            "e options.control_table); não há o que reconstruir."
        )
        return 1

    Path(cfg.bronze_dir).mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=cfg.bronze_dir, prefix=".reconstruir_") as tmp:
        # subpath=None: os diretórios de cfg já vêm com ele aplicado.
        cfg_tmp = replace(cfg, bronze_dir=tmp, subpath=None, criar_dirs=False)
        falso = _TudoPendente()
        with mock.patch("core.control.control_for", return_value=falso):
            transform(cfg_tmp)

        bronze = cfg_tmp.bronze_filepath
        if not bronze.exists() or not manifest_path(cfg_tmp).exists():
            log.error(f"Nenhuma linha no landing ({cfg.landing_dir}); nada feito.")
            return 1
        arquivos = read_manifest(cfg_tmp) + falso.sem_linhas
        linhas = _contar_linhas(bronze, cfg.bronze_sep)
        colunas = read_csv_header(bronze, cfg.bronze_sep)

        db = PostgresClient(log=log)
        linhas_tabela, colunas_tabela = _tabela(db, cfg.db_schema, cfg.db_table)
        novas = [c for c in colunas if c not in colunas_tabela]
        alvo = f"{cfg.db_schema}.{cfg.db_table}"
        log.info(f"Landing: {len(arquivos)} arquivo(s) lidos, {linhas} linha(s).")
        log.info(f"{alvo} hoje: {linhas_tabela} linha(s).")
        log.info(f"Colunas que a tabela ganha: {novas or 'nenhuma'}")

        if not confirmar:
            log.info("Simulação: nada gravado. Use --confirmar para reconstruir.")
            return 0
        if linhas < linhas_tabela and not forcar:
            log.error(
                f"O landing tem menos linhas ({linhas}) que a tabela "
                f"({linhas_tabela}): reconstruir apagaria histórico. Confira o "
                "landing ou use --forcar."
            )
            return 1

        def repor_controle(cur):
            controle.ensure(cur)
            controle.clear(cur, cfg.db_table)
            controle.register(cur, cfg.db_table, arquivos)

        db.copy_csv(
            bronze,
            cfg.db_table,
            schema=cfg.db_schema,
            sep=cfg.bronze_sep,
            write="truncate",
            filename=cfg.bronze_filepath.name,
            after_copy=repor_controle,
        )

    # O bronze-delta e o manifesto em disco já estão dentro da tabela: um load
    # avulso os copiaria de novo.
    cfg.bronze_filepath.unlink(missing_ok=True)
    write_manifest(cfg, [])
    log.info(f"📒 {alvo} reconstruída; controle com {len(arquivos)} arquivo(s).")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("config", help="caminho do <fonte>_config.yml")
    parser.add_argument("entidade", help="source do YAML")
    parser.add_argument("--confirmar", action="store_true", help="grava de verdade")
    parser.add_argument(
        "--forcar",
        action="store_true",
        help="reconstrói mesmo com menos linhas no landing do que na tabela",
    )
    args = parser.parse_args(argv)

    log = setup_logger()
    config = Path(args.config)
    etl = etls_da_fonte(config).get(args.entidade)
    if etl is None or etl.transform is None:
        log.error(f"'{args.entidade}' não tem transform em ETLS.")
        return 1
    cfg = PipelineConfig.from_yaml(config, args.entidade)
    return reconstruir(
        cfg, etl.transform, confirmar=args.confirmar, forcar=args.forcar, log=log
    )


if __name__ == "__main__":
    sys.exit(main())
