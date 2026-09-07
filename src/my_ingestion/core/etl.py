"""Orquestrador genérico de pipelines (ex-GenericETL de demodados).

Cada etapa aceita uma função custom; sem ela, cai no comportamento padrão:
extract via HttpClient, load via PostgresClient (bronze CSV -> raw.<db_table>).
"""

import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from my_ingestion.core.config import PipelineConfig
from my_ingestion.core.db import PostgresClient
from my_ingestion.core.http import HttpClient


class GenericETL:
    def __init__(
        self,
        cfg: PipelineConfig,
        extract_fn: Callable[[PipelineConfig], Path] | None = None,
        transform_fn: Callable[[PipelineConfig], None] | None = None,
        load_fn: Callable[[PipelineConfig], None] | None = None,
        log: logging.Logger | None = None,
    ):
        self.cfg = cfg
        self.extract_fn = extract_fn
        self.transform_fn = transform_fn
        self.load_fn = load_fn
        self.logger = log or logging.getLogger(self.__class__.__name__)

    # --- RUN ALL ---
    def run(self) -> None:
        self.extract()
        self.transform()
        self.load()

    # --- EXTRACT ---
    def generic_extraction(self) -> Path:
        self.logger.info("📥 Iniciando extracao...")
        extractor = HttpClient(self.logger)
        extractor.fetch_and_save(
            url=self.cfg.url_base,
            output_dir=self.cfg.landing_dir,
            filename=self.cfg.landing_file,
        )
        return self.cfg.landing_filepath

    def extract(self) -> Path:
        if self.extract_fn is not None:
            return self.extract_fn(self.cfg)
        return self.generic_extraction()

    # --- TRANSFORM ---
    def transform(self) -> None:
        if self.transform_fn is None:
            raise NotImplementedError("transform_fn não configurado em GenericETL.")
        return self.transform_fn(self.cfg)

    # --- LOAD ---
    def generic_loader(self) -> None:
        self.logger.info(
            f"📤 Carregando {self.cfg.bronze_filepath} -> {self.cfg.db_table}"
        )
        df = pd.read_csv(self.cfg.bronze_filepath, sep=";", low_memory=False)
        PostgresClient().send_df_to_db(
            df=df,
            table_name=self.cfg.db_table,
            filename=self.cfg.bronze_filepath.name,
            how="replace",
        )
        self.logger.info("✅ Carga concluida")

    def load(self) -> None:
        if self.load_fn is not None:
            return self.load_fn(self.cfg)
        self.generic_loader()
