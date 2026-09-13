"""Orquestrador do contrato único: extract → transform → load.

- ``extract``: arquivos brutos em ``cfg.landing_dir``. Sem ``extract_fn``, baixa
  ``cfg.url_base`` para ``cfg.landing_filepath``; sem ``url_base``, o landing é
  alimentado por fora (planilha, PDF, outro pipeline) e a etapa é um no-op.
- ``transform``: pré-processamento tabular que termina em ``write_bronze``.
  Sem ``transform_fn`` a etapa é um no-op (fontes JSON → JSONB).
- ``load``: por ``cfg.load`` — ``table`` (bronze CSV → ``raw_<fonte>.<tabela>``,
  full refresh), ``files`` (cada CSV do bronze_dir → tabela de mesmo nome),
  ``jsonb`` (JSONs do landing → ``JsonbLoader``), ``none`` (o orquestrador carrega).
  ``load_fn`` sobrescreve o modo.

Cada etapa lê e escreve disco, para rodar como task separada no Airflow.
"""

import argparse
import logging
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Literal

import pandas as pd

from core.config import PipelineConfig
from core.db import PostgresClient, validate_raw_schema
from core.http import HttpClient
from core.jsonb import JsonbLoader
from core.logging import setup_logger

Step = Literal["extract", "transform", "load"]
ALL_STEPS: tuple[Step, ...] = ("extract", "transform", "load")

_CHUNK = 50_000


class GenericETL:
    def __init__(
        self,
        cfg: PipelineConfig,
        *,
        extract_fn: Callable[[PipelineConfig], object] | None = None,
        transform_fn: Callable[[PipelineConfig], object] | None = None,
        load_fn: Callable[[PipelineConfig], object] | None = None,
        log: logging.Logger | None = None,
    ):
        self.cfg = cfg
        self.extract_fn = extract_fn
        self.transform_fn = transform_fn
        self.load_fn = load_fn
        self.logger = log or logging.getLogger(self.__class__.__name__)

    def run(self, steps: Sequence[Step] = ALL_STEPS) -> None:
        invalid = [s for s in steps if s not in ALL_STEPS]
        if invalid:
            raise ValueError(f"Etapa(s) inválida(s): {invalid}; use {ALL_STEPS}.")
        for step in ALL_STEPS:
            if step in steps:
                getattr(self, step)()

    # --- EXTRACT ---
    def extract(self) -> Path | None:
        if self.extract_fn is not None:
            self.logger.info("📥 Iniciando extracao...")
            return self.extract_fn(self.cfg)
        if not self.cfg.url_base:
            self.logger.info("📥 Sem url_base: landing alimentado externamente.")
            return self.cfg.landing_dir
        self.logger.info(f"📥 Baixando {self.cfg.url_base}")
        return HttpClient(self.logger).fetch_and_save(
            self.cfg.url_base, self.cfg.landing_dir, self.cfg.landing_file
        )

    # --- TRANSFORM ---
    def transform(self) -> None:
        if self.transform_fn is None:
            self.logger.info("🔄 Fonte sem transform.")
            return
        self.logger.info("🔄 Iniciando transformacao...")
        self.transform_fn(self.cfg)

    # --- LOAD ---
    def load(self) -> None:
        if self.load_fn is not None:
            self.load_fn(self.cfg)
            return
        mode = self.cfg.load
        if mode == "none":
            self.logger.info("📤 load: none (carga feita pelo orquestrador).")
            return
        getattr(self, f"_load_{mode}")()
        self.logger.info("✅ Carga concluida")

    def _load_table(self) -> None:
        cfg = self.cfg
        schema = validate_raw_schema(cfg.db_schema)
        path = cfg.bronze_filepath
        self.logger.info(f"📤 {path} -> {schema}.{cfg.db_table}")
        db = PostgresClient(log=self.logger)
        how = "replace"
        chunks = pd.read_csv(
            path, sep=cfg.bronze_sep, chunksize=_CHUNK, low_memory=False
        )
        for chunk in chunks:
            db.send_df_to_db(
                chunk, cfg.db_table, schema=schema, filename=path.name, how=how
            )
            how = "append"
        if how == "replace":  # CSV só com cabeçalho: cria a tabela vazia.
            header = pd.read_csv(path, sep=cfg.bronze_sep, nrows=0)
            db.send_df_to_db(header, cfg.db_table, schema=schema, filename=path.name)

    def _load_files(self) -> None:
        cfg = self.cfg
        schema = validate_raw_schema(cfg.db_schema)
        self.logger.info(f"📤 {cfg.bronze_dir}/* -> {schema}.*")
        PostgresClient(log=self.logger).load_files_to_table(
            cfg.bronze_dir,
            schema=schema,
            pattern=cfg.options.get("file_pattern", "*.csv"),
            sep=cfg.bronze_sep,
        )

    def _load_jsonb(self) -> None:
        cfg = self.cfg
        schema = validate_raw_schema(cfg.db_schema)
        pattern = cfg.options.get("file_pattern") or cfg.landing_file
        files = sorted(cfg.landing_dir.glob(pattern))
        if not files:
            self.logger.warning(f"⚠️ Nenhum arquivo ({pattern}) em {cfg.landing_dir}")
            return
        JsonbLoader(
            PostgresClient(log=self.logger),
            schema=schema,
            control_table=cfg.options.get("control_table", "ingestion_control"),
            log=self.logger,
        ).load_files(
            files,
            cfg.db_table,
            array_key=cfg.options.get("array_key"),
            overwrite=bool(cfg.options.get("overwrite", False)),
        )


def run_cli(build: Callable[[], GenericETL], argv: Sequence[str] | None = None) -> None:
    """Entrypoint padrão de um script: ``--steps extract,transform,load``."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--steps",
        default=",".join(ALL_STEPS),
        help="etapas a executar, separadas por vírgula (default: todas)",
    )
    args = parser.parse_args(argv)
    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    setup_logger()
    build().run(steps)


def run_many(pipelines: dict[str, Callable[[], None]], only: str | None = None) -> None:
    """Roda vários pipelines isolando falhas; ``sys.exit(1)`` se algum falhar."""
    log = setup_logger()
    if only is not None:
        if only not in pipelines:
            raise SystemExit(f"'{only}' desconhecido; opções: {', '.join(pipelines)}")
        pipelines[only]()
        return

    failures = []
    for name, fn in pipelines.items():
        try:
            log.info(f"=== Iniciando: {name} ===")
            fn()
            log.info(f"=== Concluido: {name} ===")
        except Exception:
            log.exception(f"'{name}' falhou")
            failures.append(name)

    if failures:
        log.error(f"Concluido com falhas em: {', '.join(failures)}")
        sys.exit(1)
    log.info("Todos os pipelines concluidos com sucesso")
