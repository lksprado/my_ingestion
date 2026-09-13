"""Biblioteca compartilhada do monorepo de ingestão."""

from core.config import PipelineConfig, load_yaml
from core.db import PostgresClient, validate_raw_schema
from core.etl import GenericETL, run_cli, run_many
from core.http import HttpClient
from core.incremental import (
    mark_no_data,
    missing_dates,
    missing_dates_from_db,
    pending_ids,
)
from core.io import (
    concat_files_to_df,
    list_files,
    write_bronze,
    write_bronze_streaming,
    write_csv,
)
from core.jsonb import JsonbLoader
from core.logging import setup_logger
from core.text import normalize_string, sanitize_columns, sanitize_values

__all__ = [
    "GenericETL",
    "HttpClient",
    "JsonbLoader",
    "PipelineConfig",
    "PostgresClient",
    "concat_files_to_df",
    "list_files",
    "load_yaml",
    "mark_no_data",
    "missing_dates",
    "missing_dates_from_db",
    "normalize_string",
    "pending_ids",
    "run_cli",
    "run_many",
    "sanitize_columns",
    "sanitize_values",
    "setup_logger",
    "validate_raw_schema",
    "write_bronze",
    "write_bronze_streaming",
    "write_csv",
]
