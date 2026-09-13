"""NHL — Boxscore (detalhes do jogo com jogadores). IDs da view dbt."""

from core import run_cli
from pipelines.esportes.nhl._common import build

if __name__ == "__main__":
    run_cli(lambda: build("games_details"))
    # uv run python -m pipelines.esportes.nhl.nhl_games_details [--steps load]
