"""Orquestrador dos pipelines dinâmicos da NHL (pós-dbt).

Ordem do dag_nhl_master: games_summary -> dbt (seletor nhl) -> ESTES SEIS ->
dbt de novo. Falha de um não aborta os demais.

    uv run python -m pipelines.esportes.nhl.run_all              # os seis
    uv run python -m pipelines.esportes.nhl.run_all club_stats   # um só
"""

import sys

from core import run_many
from pipelines.esportes.nhl._common import build

DYNAMIC = (
    "games_summary_details",
    "games_details",
    "play_by_play",
    "club_stats",
    "player_game_log",
    "players",
)

if __name__ == "__main__":
    run_many(
        {s: (lambda s=s: build(s).run()) for s in DYNAMIC},
        only=sys.argv[1] if len(sys.argv) > 1 else None,
    )
