"""ETL da NHL (APIs públicas) -> tabelas JSONB ``raw_nhl.nhl_raw_*``.

Sem transform: os JSONs do landing vão direto para JSONB (``load: jsonb``) e a
normalização acontece no dbt. Estáticos usam o extract padrão da core (uma
requisição a ``base_url``); dinâmicos descobrem **quais IDs requisitar** com uma
consulta na própria raw (``params_*``) e requisitam ``base_url.format(**linha)``.

Nada de dbt no caminho: as consultas de parâmetro leem o schema da carga
(``PostgresClient()``, sem ``models_target``) — as tabelas ``nhl_raw_*`` e a de
controle ``nhl_ingestion_control``, que registra cada arquivo já ingerido.

Ordem: ``games_summary`` (base dos IDs) -> os seis dinâmicos. ``seasons`` e
``teams`` mudam uma vez por ano; ``teams`` é pré-requisito de ``club_stats``.
"""

import logging
from collections.abc import Callable
from functools import partial
from pathlib import Path

from core import (
    Etl,
    HttpClient,
    JsonbLoader,
    PipelineConfig,
    PostgresClient,
    run_source,
    validate_raw_schema,
)

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "nhl_config.yml"

# Tabelas da raw consultadas para montar os parâmetros (nomes fixos da fonte).
T_SEASONS = "nhl_raw_all_seasons_id"
T_TEAMS = "nhl_raw_all_teams_id"
T_GAMES_SUMMARY = "nhl_raw_all_games_summary"
T_CLUB_STATS = "nhl_raw_all_club_stats"

# Jogo que consta da agenda oficial (1) e já terminou (7).
AGENDADO = "(payload ->> 'gameScheduleStateId')::INT = 1"
REALIZADO = "(payload ->> 'gameStateId')::INT = 7"


# ------------------------- consultas de parâmetros -------------------------


def _schema(cfg: PipelineConfig) -> str:
    """Schema da raw, validado antes de ser interpolado no SQL."""
    return validate_raw_schema(cfg.db_schema)


def _query(sql: str, columns: list[str]) -> list[dict]:
    """Executa ``sql`` no banco da carga e devolve as linhas como dicts."""
    rows = PostgresClient(log=logger).read_sql(sql)[columns].to_dict(orient="records")
    logger.info(f"🔎 {len(rows)} parâmetro(s) a requisitar")
    return rows


def params_jogos(cfg: PipelineConfig) -> list[dict]:
    """``game_id`` dos jogos já realizados na temporada atual que faltam carregar.

    "Faltam carregar" = não estão registrados em ``cfg.db_table`` na tabela de
    controle, cujo ``filename`` carrega o ``game_id`` (``raw_<id>_details.json``,
    ``raw_<id>_summary_details.json``, ``raw_<id>.json``).
    """
    s = _schema(cfg)
    sql = f"""
    WITH temporada_atual AS (
        SELECT MAX(payload::INT) AS season_id
        FROM {s}.{T_SEASONS}
    ),
    jogos_realizados AS (
        SELECT DISTINCT (payload ->> 'id')::BIGINT AS game_id
        FROM {s}.{T_GAMES_SUMMARY}
        WHERE {AGENDADO}
          AND {REALIZADO}
          AND (payload ->> 'season')::INT
              = (SELECT season_id FROM temporada_atual)
    ),
    ja_ingeridos AS (
        SELECT (REGEXP_MATCH(filename, '([0-9]+)'))[1]::BIGINT AS game_id
        FROM {s}.{cfg.options["control_table"]}
        WHERE table_schema = '{s}'
          AND table_name = '{cfg.db_table}'
    )
    SELECT g.game_id
    FROM jogos_realizados AS g
    WHERE NOT EXISTS (
        SELECT 1 FROM ja_ingeridos AS i WHERE i.game_id = g.game_id
    )
    ORDER BY g.game_id
    """
    return _query(sql, ["game_id"])


def params_times(cfg: PipelineConfig) -> list[dict]:
    """``team_id`` (triCode), ``season_id`` e ``game_type_id`` da temporada atual.

    A URL de ``club-stats`` espera a sigla do time, não o id numérico: os times
    que jogaram vêm de ``games_summary`` e a sigla de ``teams`` (id 70 é o
    "placeholder" da NHL e fica de fora, como no staging do dbt).
    """
    s = _schema(cfg)
    sql = f"""
    WITH jogos_realizados AS (
        SELECT
            (payload ->> 'season')::INT         AS season_id,
            (payload ->> 'gameType')::INT       AS game_type_id,
            (payload ->> 'homeTeamId')::INT     AS home_team_id,
            (payload ->> 'visitingTeamId')::INT AS visiting_team_id
        FROM {s}.{T_GAMES_SUMMARY}
        WHERE {AGENDADO} AND {REALIZADO}
    ),
    temporada_atual AS (
        SELECT MAX(season_id) AS season_id FROM jogos_realizados
    ),
    jogos AS (
        SELECT * FROM jogos_realizados
        WHERE game_type_id IN (2, 3)
          AND season_id = (SELECT season_id FROM temporada_atual)
    ),
    times_por_jogo AS (
        SELECT season_id, game_type_id, home_team_id AS nhl_team_id FROM jogos
        UNION
        SELECT season_id, game_type_id, visiting_team_id FROM jogos
    ),
    times AS (
        SELECT
            (payload ->> 'id')::INT AS nhl_team_id,
            (payload ->> 'triCode') AS team_id
        FROM {s}.{T_TEAMS}
        WHERE (payload ->> 'id')::INT <> 70
    )
    SELECT DISTINCT t.team_id, j.season_id, j.game_type_id
    FROM times_por_jogo AS j
    INNER JOIN times AS t ON t.nhl_team_id = j.nhl_team_id
    ORDER BY t.team_id, j.season_id DESC
    """
    return _query(sql, ["team_id", "season_id", "game_type_id"])


def _cte_jogadores(s: str) -> str:
    """CTEs ``base`` (temporada/tipo de jogo atuais) e ``jogadores``.

    A temporada vem da **agenda** (``games_summary``), não da tabela de seasons:
    o endpoint de seasons já lista a temporada seguinte, que ainda não tem
    ``club_stats`` e zeraria a lista de jogadores.
    """
    return f"""
    WITH base AS (
        SELECT
            (payload ->> 'season')::INT        AS season_id,
            MAX((payload ->> 'gameType')::INT) AS game_type_id
        FROM {s}.{T_GAMES_SUMMARY}
        WHERE {AGENDADO}
          AND (payload ->> 'season')::INT = (
              SELECT MAX((payload ->> 'season')::INT)
              FROM {s}.{T_GAMES_SUMMARY}
              WHERE {AGENDADO}
          )
        GROUP BY 1
    ),
    club_stats AS (
        SELECT
            (payload ->> 'season')::INT   AS season_id,
            (payload ->> 'gameType')::INT AS game_type_id,
            payload
        FROM {s}.{T_CLUB_STATS}
    ),
    jogadores AS (
        SELECT DISTINCT (p ->> 'playerId')::INT AS player_id
        FROM club_stats AS c
        INNER JOIN base AS b
            ON c.season_id = b.season_id
            AND c.game_type_id = b.game_type_id,
            LATERAL JSONB_ARRAY_ELEMENTS(
                COALESCE(c.payload -> 'goalies', '[]'::JSONB)
                || COALESCE(c.payload -> 'skaters', '[]'::JSONB)
            ) AS p
    )
    """


def params_jogadores(cfg: PipelineConfig) -> list[dict]:
    """``player_id`` de goleiros e jogadores de linha da temporada atual."""
    sql = (
        _cte_jogadores(_schema(cfg))
        + """
    SELECT player_id FROM jogadores ORDER BY player_id
    """
    )
    return _query(sql, ["player_id"])


def params_jogadores_temporada(cfg: PipelineConfig) -> list[dict]:
    """``player_id`` com a ``season_id``/``game_type_id`` atuais (game log)."""
    sql = (
        _cte_jogadores(_schema(cfg))
        + """
    SELECT j.player_id, b.season_id, b.game_type_id
    FROM jogadores AS j
    CROSS JOIN base AS b
    ORDER BY j.player_id
    """
    )
    return _query(sql, ["player_id", "season_id", "game_type_id"])


# ------------------------------ extract/load ------------------------------


def extract_dynamic(
    cfg: PipelineConfig, params: Callable[[PipelineConfig], list[dict]]
) -> None:
    """Um JSON por linha de parâmetros: ``url_base``/``landing_file`` com ``{col}``.

    Com ``season_subdir`` os arquivos vão para ``landing_dir/<season_id>/``; com
    ``skip_existing`` (dados imutáveis, ``overwrite: false``) não rebaixa o que já
    está no landing.
    """
    rows = params(cfg)
    if not rows:
        logger.info("Nada a extrair.")
        return

    season_subdir = bool(cfg.options.get("season_subdir"))
    skip_existing = bool(cfg.options.get("skip_existing"))

    groups: dict[Path, list[tuple[str, str]]] = {}
    pulados = 0
    for row in rows:
        out_dir = (
            cfg.landing_dir / str(row["season_id"])
            if season_subdir
            else cfg.landing_dir
        )
        filename = cfg.landing_file.format(**row)
        if skip_existing and (out_dir / filename).exists():
            pulados += 1
            continue
        groups.setdefault(out_dir, []).append((cfg.url_base.format(**row), filename))

    if pulados:
        logger.info(f"⏭️ {pulados} arquivo(s) já no landing.")
    if not groups:
        logger.info("Nada a extrair.")
        return

    # API pública sem auth; retry curto para não travar lotes de milhares de IDs.
    http = HttpClient(logger, retries=3, backoff_factor=0.5, timeout=10)
    workers = int(cfg.options.get("workers", 1))
    total = sum(len(t) for t in groups.values())
    logger.info(f"📥 Extraindo {total} arquivo(s) para {cfg.landing_dir}")
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


ETLS = {
    # estáticos: 1 request, full refresh
    "seasons": Etl(),
    "teams": Etl(),
    "games_summary": Etl(),
    # dinâmicos: IDs de uma consulta na raw (params_*), sem dbt no meio
    "games_summary_details": Etl(extract=partial(extract_dynamic, params=params_jogos)),
    "games_details": Etl(extract=partial(extract_dynamic, params=params_jogos)),
    "play_by_play": Etl(extract=partial(extract_dynamic, params=params_jogos)),
    "club_stats": Etl(extract=partial(extract_dynamic, params=params_times)),
    "player_game_log": Etl(
        extract=partial(extract_dynamic, params=params_jogadores_temporada),
        load=load_latest_season,
    ),
    "players": Etl(extract=partial(extract_dynamic, params=params_jogadores)),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.esportes.nhl.nhl_etl [entidade ...] [--steps ...]
