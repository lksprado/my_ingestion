"""NHL — Ficha dos jogadores. IDs da view dbt."""

from core import run_cli
from pipelines.esportes.nhl._common import build

if __name__ == "__main__":
    run_cli(lambda: build("players"))
    # uv run python -m pipelines.esportes.nhl.nhl_players [--steps load]
