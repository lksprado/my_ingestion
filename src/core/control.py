"""Controle de quais arquivos de landing já entraram numa tabela raw.

Generaliza a tabela que o ``JsonbLoader`` mantinha só para a NHL: uma linha por
``(schema, tabela, arquivo)``, o que torna qualquer carga incremental idempotente
— arquivo já registrado não volta.

Fontes que extraem por ID (legislativo) baixam um JSON por unidade imutável, mas
o ``transform`` reconstruía o bronze a partir de **todo** o landing e o ``load``
reescrevia a tabela inteira. Com o controle, o par vira:

- ``write_bronze_incremental(cfg, files, parse_fn)`` no transform: só os arquivos
  ainda não registrados entram no bronze, e um **manifesto** ao lado do bronze diz
  quais foram;
- ``write: append`` no YAML: o load faz o COPY do bronze-delta e registra o
  manifesto **na mesma transação**. Falha em qualquer ponto → rollback, nada
  registrado, o transform seguinte refaz o mesmo delta.

Nessas fontes o bronze passa a ser o delta da última execução, não o histórico
completo — a tabela raw é que acumula.
"""

import csv
import logging
from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd

from core.config import PipelineConfig
from core.db import (
    LOADED_AT_DEFAULT,
    PostgresClient,
    ensure_column_default,
    validate_raw_schema,
)
from core.io import write_bronze_streaming

logger = logging.getLogger(__name__)

CONTROL_TABLE = "ingestion_control"
MANIFEST_SUFFIX = ".manifesto.csv"


class IngestionControl:
    """Registro dos arquivos já ingeridos em ``<schema>.<control_table>``."""

    def __init__(
        self,
        db: PostgresClient,
        *,
        schema: str,
        table: str = CONTROL_TABLE,
        log: logging.Logger | None = None,
    ):
        self.db = db
        self.schema = validate_raw_schema(schema)
        self.table = table
        self.logger = log or logger

    def ensure(self, cur) -> None:
        # `ingested_at` não é o `loaded_at_utc` das tabelas de dado: aqui a linha
        # é o registro de um arquivo no manifesto, e a idempotência é por
        # `filename` (ninguém lê este carimbo). Só o fuso é o mesmo — UTC
        # explícito, para não depender do TimeZone do servidor.
        cur.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema};
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
                table_schema TEXT NOT NULL,
                table_name   TEXT NOT NULL,
                filename     TEXT NOT NULL,
                ingested_at  TIMESTAMP NOT NULL
                             DEFAULT ({LOADED_AT_DEFAULT}),
                is_overwrite BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (table_schema, table_name, filename)
            );
            """
        )
        # Tabela de controle criada antes desta regra ficou com o `now()` puro.
        ensure_column_default(cur, self.schema, self.table, "ingested_at", None)

    def ingested(self, cur, table: str) -> set[str]:
        cur.execute(
            f"SELECT filename FROM {self.schema}.{self.table} "
            "WHERE table_schema = %s AND table_name = %s",
            (self.schema, table),
        )
        return {row[0] for row in cur.fetchall()}

    def register(
        self, cur, table: str, filenames: Iterable[str], overwrite: bool = False
    ) -> None:
        valores = [(self.schema, table, nome, overwrite) for nome in filenames]
        if not valores:
            return
        cur.executemany(
            f"INSERT INTO {self.schema}.{self.table} "
            "(table_schema, table_name, filename, is_overwrite) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            valores,
        )

    def clear(self, cur, table: str) -> None:
        cur.execute(
            f"DELETE FROM {self.schema}.{self.table} "
            "WHERE table_schema = %s AND table_name = %s",
            (self.schema, table),
        )

    def pending(self, files: Iterable[Path], table: str) -> list[Path]:
        """Arquivos ainda não registrados, em conexão própria (só leitura)."""
        files = sorted(Path(f) for f in files)
        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                self.ensure(cur)
                ja_feitos = self.ingested(cur, table)
            conn.commit()
        finally:
            if not self.db.external_connection:
                conn.close()
        pendentes = [f for f in files if f.name not in ja_feitos]
        self.logger.info(
            f"{len(pendentes)} arquivo(s) pendente(s) de {len(files)} "
            f"para {self.schema}.{table}."
        )
        return pendentes


# ------------------------ manifesto ------------------------


def manifest_path(cfg: PipelineConfig) -> Path:
    """Arquivo que lista o landing consumido pelo bronze atual."""
    bronze = cfg.bronze_filepath
    return bronze.with_name(bronze.stem + MANIFEST_SUFFIX)


def write_manifest(cfg: PipelineConfig, files: Iterable[Path]) -> Path:
    path = manifest_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["filename"])
        for arquivo in files:
            writer.writerow([Path(arquivo).name])
    return path


def read_manifest(cfg: PipelineConfig) -> list[str]:
    """Nomes do manifesto; lista vazia se ele não existe."""
    path = manifest_path(cfg)
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        linhas = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]
    return [nome for nome in linhas if nome != "filename"]


def control_for(cfg: PipelineConfig, log: logging.Logger | None = None):
    """``IngestionControl`` da fonte, ou ``None`` se o YAML não pediu controle."""
    nome = cfg.options.get("control_table")
    if not nome:
        return None
    return IngestionControl(
        PostgresClient(log=log), schema=cfg.db_schema, table=nome, log=log
    )


def write_bronze_incremental(
    cfg: PipelineConfig,
    files: Iterable[Path],
    parse_fn: Callable[[Path], pd.DataFrame | None],
    log: logging.Logger | None = None,
) -> Path | None:
    """Bronze só com os arquivos de landing ainda não carregados, mais o manifesto.

    Sem ``options.control_table`` no YAML, cai no ``write_bronze_streaming``
    normal (rebuild completo) — assim a função serve aos dois casos.
    """
    log = log or logger
    controle = control_for(cfg, log)
    if controle is None:
        return write_bronze_streaming(cfg, files, parse_fn)

    pendentes = controle.pending(files, cfg.db_table)
    if not pendentes:
        log.info("✅ Nada novo no landing; bronze-delta vazio.")
        write_manifest(cfg, [])
        return None
    path = write_bronze_streaming(cfg, pendentes, parse_fn)
    write_manifest(cfg, pendentes)
    return path
