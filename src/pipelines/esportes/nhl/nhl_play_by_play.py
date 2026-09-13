"""NHL — Play-by-play (eventos do jogo). IDs da view dbt."""

from core import run_cli
from pipelines.esportes.nhl._common import build

if __name__ == "__main__":
    run_cli(lambda: build("play_by_play"))
    # uv run python -m pipelines.esportes.nhl.nhl_play_by_play [--steps load]
