"""Orquestrador dos pipelines dinâmicos da NHL (pós-dbt).

Ordem do dag_nhl_master: games_summary -> dbt (seletor nhl) -> ESTES SEIS ->
dbt de novo. Falha de um não aborta os demais.

    uv run python -m pipelines.esportes.nhl.run_all              # os seis
    uv run python -m pipelines.esportes.nhl.run_all club_stats   # um só
"""

import sys

from core import setup_logger
from pipelines.esportes.nhl._common import run_dynamic

logger = setup_logger("my_ingestion")

DYNAMIC = (
    "games_summary_details",
    "games_details",
    "play_by_play",
    "club_stats",
    "player_game_log",
    "players",
)


def main(only: str | None = None) -> None:
    sources = (only,) if only else DYNAMIC
    failures = []
    for source in sources:
        try:
            logger.info(f"=== NHL: {source} ===")
            run_dynamic(source, logger)
        except Exception:
            logger.exception(f"Pipeline '{source}' falhou")
            failures.append(source)

    if failures:
        logger.error(f"Concluido com falhas em: {', '.join(failures)}")
        sys.exit(1)
    logger.info("Todos os pipelines NHL concluidos")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
