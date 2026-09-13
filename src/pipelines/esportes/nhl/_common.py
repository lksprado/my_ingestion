"""Helpers dos scripts da NHL (um script por tabela destino).

Fluxo: extract (JSON por ID no landing; IDs vêm de uma view do dbt) -> load
``jsonb`` (core). Não há transform: a normalização acontece no dbt.
"""

import logging
from pathlib import Path

from core import GenericETL, HttpClient, JsonbLoader, PipelineConfig, PostgresClient

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "nhl_config.yml"


def fetch_params(cfg: PipelineConfig) -> list[dict]:
    """Linhas da view ``options.param_view`` como dicts (colunas -> valores).

    ``param_filter`` restringe a ``WHERE <col> IS FALSE`` (IDs não sincronizados).
    """
    opts = cfg.options
    columns = list(opts["param_columns"])
    sql = (
        f"SELECT DISTINCT {', '.join(columns)} "
        f"FROM {opts['param_schema']}.{opts['param_view']}"
    )
    if opts.get("param_filter"):
        sql += f" WHERE {opts['param_filter']} IS FALSE"
    rows = PostgresClient(log=logger).read_sql(sql)[columns].to_dict(orient="records")
    logger.info(f"🔎 {len(rows)} parâmetro(s) de {opts['param_view']}")
    return rows


def extract_dynamic(cfg: PipelineConfig) -> None:
    """Um JSON por linha de parâmetros: ``url_base``/``landing_file`` com ``{col}``.

    Com ``season_subdir`` os arquivos vão para ``landing_dir/<season_id>/``.
    """
    rows = fetch_params(cfg)
    if not rows:
        logger.info("Nada a extrair.")
        return

    # API pública sem auth; retry curto para não travar lotes de milhares de IDs.
    http = HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
    workers = int(cfg.options.get("workers", 1))
    season_subdir = bool(cfg.options.get("season_subdir"))

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
    logger.info(f"📥 Extraindo {len(rows)} arquivo(s) para {cfg.landing_dir}")
    for out_dir, tasks in groups.items():
        http.fetch_and_save_many(tasks, out_dir, workers=workers)


def latest_season_files(cfg: PipelineConfig) -> list[Path]:
    """Arquivos da subpasta de temporada mais recente do landing."""
    seasons = [d for d in cfg.landing_dir.iterdir() if d.is_dir()]
    if not seasons:
        return []
    latest = max(seasons, key=lambda p: p.name)
    return sorted(latest.glob(cfg.options.get("file_pattern") or cfg.landing_file))


def load_latest_season(cfg: PipelineConfig) -> None:
    """Load ``jsonb`` restrito à temporada mais recente (``season_subdir``)."""
    files = latest_season_files(cfg)
    if not files:
        logger.warning(f"⚠️ Nenhuma temporada em {cfg.landing_dir}")
        return
    JsonbLoader(
        PostgresClient(log=logger),
        schema=cfg.db_schema,
        control_table=cfg.options["control_table"],
        log=logger,
    ).load_files(
        files,
        cfg.db_table,
        array_key=cfg.options.get("array_key"),
        overwrite=bool(cfg.options.get("overwrite", False)),
    )


def build(source: str) -> GenericETL:
    """Estáticos: extract padrão (url_base). Dinâmicos (``param_view``): por ID."""
    cfg = PipelineConfig.from_yaml(CONFIG_FILE, source)
    return GenericETL(
        cfg,
        extract_fn=extract_dynamic if cfg.options.get("param_view") else None,
        load_fn=load_latest_season if cfg.options.get("season_subdir") else None,
        log=logger,
    )
