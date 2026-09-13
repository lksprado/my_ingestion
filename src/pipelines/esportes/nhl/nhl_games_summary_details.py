"""NHL — Right-rail (estatísticas do jogo). IDs da view dbt."""

from core import run_cli
from pipelines.esportes.nhl._common import build

if __name__ == "__main__":
    run_cli(lambda: build("games_summary_details"))
    # uv run python -m pipelines.esportes.nhl.nhl_games_summary_details [--steps load]
