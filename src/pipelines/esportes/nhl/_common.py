"""Helpers compartilhados pelos scripts da NHL (um script por tabela destino).

Fluxo de cada script: ``fetch_params`` (IDs de uma view dbt) -> ``extract``
(JSON por ID no landing) -> ``load`` (JSON -> JSONB via ``JsonbLoader``).
Não há transform: a normalização acontece no dbt (my_datawarehouse).
"""

import logging
from pathlib import Path

from core import (
    HttpClient,
    JsonbLoader,
    PipelineConfig,
    PostgresClient,
    load_source_config,
)

CONFIG_FILE = Path(__file__).parent / "nhl_config.yml"
# Preservado do repo original: o banco já tem o histórico de ingestão aqui.
CONTROL_TABLE = "nhl_ingestion_control"
# Views de parâmetro do dbt (leitura), no mesmo banco do ENV ativo.
PARAM_SCHEMA = "staging"


def _db(logger: logging.Logger) -> PostgresClient:
    return PostgresClient(log=logger)


def make_config(source: str) -> PipelineConfig:
    return PipelineConfig(**load_source_config(CONFIG_FILE, source=source))


def _http(logger: logging.Logger) -> HttpClient:
    # API pública sem auth; retry curto para não travar lotes de milhares de IDs.
    return HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)


def fetch_params(cfg: PipelineConfig, logger: logging.Logger) -> list[dict]:
    """Linhas da view ``options.param_view`` como dicts (colunas -> valores).

    ``param_filter`` restringe a ``WHERE <col> IS FALSE`` (IDs não sincronizados).
    """
    opts = cfg.options
    columns = list(opts["param_columns"])
    sql = (
        f"SELECT DISTINCT {', '.join(columns)} FROM {PARAM_SCHEMA}.{opts['param_view']}"
    )
    if opts.get("param_filter"):
        sql += f" WHERE {opts['param_filter']} IS FALSE"

    df = _db(logger).fetchall(sql)
    rows = df[columns].to_dict(orient="records")
    logger.info(f"🔎 {len(rows)} parâmetro(s) de {opts['param_view']}")
    return rows


def extract(cfg: PipelineConfig, rows: list[dict], logger: logging.Logger) -> None:
    """Requisita um JSON por linha de ``rows`` e grava em ``cfg.landing_dir``.

    Com ``season_subdir`` os arquivos vão para ``landing_dir/<season_id>/``.
    Falhas individuais são logadas e puladas (HttpClient devolve None).
    """
    if not rows:
        logger.info("Nada a extrair.")
        return

    http = _http(logger)
    workers = int(cfg.options.get("workers", 1))
    season_subdir = bool(cfg.options.get("season_subdir"))

    # Agrupa por diretório de saída (fetch_and_save_many trabalha num só).
    groups: dict[Path, list[tuple[str, str]]] = {}
    for row in rows:
        out_dir = (
            cfg.landing_dir / str(row["season_id"])
            if season_subdir
            else cfg.landing_dir
        )
        groups.setdefault(out_dir, []).append(
            (cfg.url_base.format(**row), cfg.landing_file.format(**row))
        )

    total = len(rows)
    logger.info(f"📥 Extraindo {total} arquivo(s) para {cfg.landing_dir}")
    for out_dir, tasks in groups.items():
        http.fetch_and_save_many(tasks, out_dir, workers=workers)
    logger.info("✅ Extração concluída")


def extract_single(cfg: PipelineConfig, logger: logging.Logger) -> None:
    """Fontes estáticas: uma requisição para ``cfg.landing_filepath``."""
    _http(logger).fetch_and_save(cfg.url_base, cfg.landing_dir, cfg.landing_file)


def _landing_files(cfg: PipelineConfig) -> list[Path]:
    pattern = cfg.options.get("file_pattern") or cfg.landing_file
    if cfg.options.get("season_subdir"):
        seasons = [d for d in cfg.landing_dir.iterdir() if d.is_dir()]
        if not seasons:
            return []
        latest = max(seasons, key=lambda p: p.name)
        return sorted(latest.glob(pattern))
    return sorted(cfg.landing_dir.glob(pattern))


def load(cfg: PipelineConfig, logger: logging.Logger) -> None:
    """Carrega os JSONs do landing em ``<db_schema>.<db_table>`` (JSONB)."""
    files = _landing_files(cfg)
    if not files:
        logger.warning(f"⚠️ Nenhum arquivo em {cfg.landing_dir}")
        return
    loader = JsonbLoader(
        _db(logger), schema=cfg.db_schema, control_table=CONTROL_TABLE, log=logger
    )
    loader.load_files(
        files,
        cfg.db_table,
        array_key=cfg.options.get("array_key"),
        overwrite=bool(cfg.options.get("overwrite", False)),
    )


def run_static(source: str, logger: logging.Logger) -> None:
    cfg = make_config(source)
    extract_single(cfg, logger)
    load(cfg, logger)


def run_dynamic(
    source: str, logger: logging.Logger, skip_extract: bool = False
) -> None:
    cfg = make_config(source)
    if not skip_extract:
        extract(cfg, fetch_params(cfg, logger), logger)
    load(cfg, logger)
