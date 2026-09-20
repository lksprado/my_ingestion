"""Biblioteca compartilhada do monorepo de ingestão."""

from core.config import PipelineConfig, load_yaml
from core.control import (
    IngestionControl,
    read_manifest,
    write_bronze_incremental,
    write_manifest,
)
from core.db import PostgresClient, validate_raw_schema, validate_write_mode
from core.etl import Etl, GenericETL, build_etl, run_source
from core.http import HttpClient
from core.incremental import (
    extract_by_ids,
    landing_ids,
    mark_no_data,
    missing_dates,
    missing_dates_from_db,
    pending_ids,
    read_ids,
)
from core.io import (
    concat_files_to_df,
    concat_landing,
    integral_floats_to_int,
    list_files,
    reset_bronze,
    write_bronze,
    write_bronze_streaming,
    write_csv,
)
from core.jsonb import JsonbLoader
from core.logging import setup_logger
from core.parsers.json import flatten_children
from core.text import normalize_string, sanitize_columns, sanitize_values

__all__ = [
    "write_manifest",
    "write_bronze_incremental",
    "read_manifest",
    "IngestionControl",
    "Etl",
    "GenericETL",
    "HttpClient",
    "JsonbLoader",
    "PipelineConfig",
    "PostgresClient",
    "build_etl",
    "concat_files_to_df",
    "concat_landing",
    "extract_by_ids",
    "flatten_children",
    "integral_floats_to_int",
    "landing_ids",
    "list_files",
    "load_yaml",
    "mark_no_data",
    "missing_dates",
    "missing_dates_from_db",
    "normalize_string",
    "pending_ids",
    "read_ids",
    "reset_bronze",
    "run_source",
    "sanitize_columns",
    "sanitize_values",
    "setup_logger",
    "validate_raw_schema",
    "validate_write_mode",
    "write_bronze",
    "write_bronze_streaming",
    "write_csv",
]
