"""NHL — Game log dos jogadores na temporada atual. IDs da view dbt."""

from core import run_cli
from pipelines.esportes.nhl._common import build

if __name__ == "__main__":
    run_cli(lambda: build("player_game_log"))
    # uv run python -m pipelines.esportes.nhl.nhl_player_game_log [--steps load]
