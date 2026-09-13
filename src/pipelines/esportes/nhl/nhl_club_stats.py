"""NHL — Estatísticas de jogadores por time/temporada/tipo. IDs da view dbt."""

from core import run_cli
from pipelines.esportes.nhl._common import build

if __name__ == "__main__":
    run_cli(lambda: build("club_stats"))
    # uv run python -m pipelines.esportes.nhl.nhl_club_stats [--steps load]
