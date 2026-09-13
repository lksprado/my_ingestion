"""Orquestrador das ETLs de investimentos (ex-investments/src/main.py).

Roda b3, avenue e google em sequência (falha de uma não aborta as outras).
FGC roda à parte (depende da camada intermediate do DW):

    python -m pipelines.financas.investimentos.run_all          # todas
    python -m pipelines.financas.investimentos.run_all b3       # uma só
    python -m pipelines.financas.investimentos.run_all fgc
"""

import sys
from pathlib import Path

from core import setup_logger
from core.db import PostgresClient
from pipelines.financas.investimentos.avenue_etl import (
    run_avenue_dividends_etl,
    run_avenue_etl,
)
from pipelines.financas.investimentos.b3_etl import run_consolidation
from pipelines.financas.investimentos.fgc_etl import build_depara
from pipelines.financas.investimentos.google_finance_etl import (
    run_google_finance_etl,
)
from settings import settings

logger = setup_logger("my_ingestion")

RAW_BASE = settings.lake_root / "raw" / "investments"
BRONZE_BASE = settings.lake_root / "bronze" / "investments"
CONFIG_PATH = Path(__file__).parent / "config.yml"

# Um schema por fonte; a tabela vem do nome do arquivo bronze (modo stem).
SCHEMA_B3 = "raw_b3"
SCHEMA_AVENUE = "raw_avenue"
SCHEMA_GOOGLE = "raw_google"


def run_b3() -> None:
    output_dir = BRONZE_BASE / "b3"
    run_consolidation(input_dir=RAW_BASE / "b3", output_dir=output_dir)
    PostgresClient(log=logger).load_files_to_table(
        output_dir, schema=SCHEMA_B3, strip_prefix="consolidado_"
    )


def run_avenue() -> None:
    input_dir = RAW_BASE / "avenue"
    output_dir = BRONZE_BASE / "avenue"
    run_avenue_etl(input_dir=input_dir, output_dir=output_dir)
    run_avenue_dividends_etl(input_dir=input_dir, output_dir=output_dir)
    PostgresClient(log=logger).load_files_to_table(output_dir, schema=SCHEMA_AVENUE)


def run_google() -> None:
    output_dir = BRONZE_BASE / "google"
    run_google_finance_etl(
        output_dir=output_dir,
        credentials=str(settings.google_credentials_file),
        config_path=CONFIG_PATH,
    )
    PostgresClient(log=logger).load_files_to_table(
        output_dir, schema=SCHEMA_GOOGLE, pattern="google_*.csv", strip_prefix="google_"
    )


def run_fgc() -> None:
    """De-para emissor -> conglomerado prudencial (roda após o intermediate do DW)."""
    build_depara(
        db_url=settings.db_url,
        csv_path=RAW_BASE / "instituicoes" / "instituicoes_conglomerado_prudencial.csv",
        output_dir=settings.seeds_root,
    )


ETLS = {"b3": run_b3, "avenue": run_avenue, "google": run_google, "fgc": run_fgc}


def main(only: str | None = None) -> None:
    if only is not None:
        ETLS[only]()
        return

    failures = []
    for name in ("b3", "avenue", "google"):
        try:
            logger.info("=== Iniciando ETL: %s ===", name)
            ETLS[name]()
            logger.info("=== ETL '%s' concluida ===", name)
        except Exception:
            logger.exception("ETL '%s' falhou", name)
            failures.append(name)

    if failures:
        logger.error("Concluido com falhas em: %s", ", ".join(failures))
        sys.exit(1)
    logger.info("Todas as ETLs concluidas com sucesso")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
