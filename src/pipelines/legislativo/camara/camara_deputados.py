"""Ficha de cada deputado (IDs de parameters/id_deputados.csv, full refresh)."""

import logging
from pathlib import Path

from core import (
    GenericETL,
    HttpClient,
    PipelineConfig,
    run_cli,
    sanitize_columns,
    sanitize_values,
    write_bronze,
)
from core.parsers.json import normalize_json_object
from pipelines.legislativo._common import concat_landing, read_ids

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"

_KEEP_VALUES = [
    "uri",
    "urlwebsite",
    "redesocial",
    "datanascimento",
    "datafalecimento",
    "ultimostatus_uri",
    "ultimostatus_uripartido",
    "ultimostatus_urlfoto",
    "ultimostatus_email",
    "ultimostatus_data",
    "ultimostatus_gabinete_telefone",
    "ultimostatus_gabinete_email",
]


def extract(cfg: PipelineConfig) -> None:
    ids = read_ids(cfg.parameter_filepath, "id")
    tasks = [(cfg.url_base.format(id=i), cfg.landing_file.format(id=i)) for i in ids]
    HttpClient(logger).fetch_and_save_many(
        tasks, cfg.landing_dir, workers=int(cfg.options.get("workers", 1))
    )


def _parse(path: Path):
    df = normalize_json_object(path, "dados")
    if df.empty:
        return None
    return sanitize_values(sanitize_columns(df), exclude=_KEEP_VALUES)


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, _parse))


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "deputados")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.camara.camara_deputados
