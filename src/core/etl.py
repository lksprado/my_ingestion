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

Uma fonte = um ``<fonte>_etl.py`` com ``ETLS = {"<entidade>": Etl(...)}`` (ordem =
ordem de execução) e ``run_source(CONFIG_FILE, ETLS)`` no ``__main__``.
"""

import argparse
import logging
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
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


@dataclass(frozen=True)
class Etl:
    """Funções de uma entidade; ``None`` usa o default do ``GenericETL``."""

    extract: Callable[[PipelineConfig], object] | None = None
    transform: Callable[[PipelineConfig], object] | None = None
    load: Callable[[PipelineConfig], object] | None = None


def build_etl(config_file: Path | str, source: str, etl: Etl) -> GenericETL:
    """``GenericETL`` do ``source`` do YAML com as funções de ``etl``."""
    cfg = PipelineConfig.from_yaml(config_file, source)
    fonte = Path(config_file).stem.removesuffix("_config")
    return GenericETL(
        cfg,
        extract_fn=etl.extract,
        transform_fn=etl.transform,
        load_fn=etl.load,
        log=logging.getLogger(f"{fonte}.{source}"),
    )


def run_source(
    config_file: Path | str,
    etls: dict[str, Etl],
    argv: Sequence[str] | None = None,
) -> None:
    """Entrypoint de um ``<fonte>_etl.py``: ``[entidade ...] [--steps ...]``.

    Sem entidades, roda todas na ordem de ``etls``. Com mais de uma, a falha de
    uma não aborta as demais e o processo termina com ``sys.exit(1)``.
    """
    parser = argparse.ArgumentParser(
        description=f"ETL de {Path(config_file).stem.removesuffix('_config')}"
    )
    parser.add_argument(
        "entidades",
        nargs="*",
        metavar="entidade",
        help=f"default: todas, nesta ordem: {' '.join(etls)}",
    )
    parser.add_argument(
        "--steps",
        default=",".join(ALL_STEPS),
        help="etapas a executar, separadas por vírgula (default: todas)",
    )
    args = parser.parse_args(argv)
    unknown = [e for e in args.entidades if e not in etls]
    if unknown:
        parser.error(f"entidade(s) desconhecida(s) {unknown}; opções: {list(etls)}")
    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    invalid = [s for s in steps if s not in ALL_STEPS]
    if invalid:
        parser.error(f"etapa(s) inválida(s) {invalid}; opções: {list(ALL_STEPS)}")

    log = setup_logger()
    names = args.entidades or list(etls)
    if len(names) == 1:
        build_etl(config_file, names[0], etls[names[0]]).run(steps)
        return

    failures = []
    for name in names:
        try:
            log.info(f"=== Iniciando: {name} ===")
            build_etl(config_file, name, etls[name]).run(steps)
            log.info(f"=== Concluido: {name} ===")
        except Exception:
            log.exception(f"'{name}' falhou")
            failures.append(name)
    if failures:
        log.error(f"Concluido com falhas em: {', '.join(failures)}")
        sys.exit(1)
    log.info("Todas as entidades concluidas com sucesso")


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
