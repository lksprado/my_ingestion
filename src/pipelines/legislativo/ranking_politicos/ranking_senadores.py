"""Ranking dos Políticos: senadores."""

from core import run_cli
from pipelines.legislativo.ranking_politicos._common import build

if __name__ == "__main__":
    run_cli(lambda: build("senadores"))
    # uv run python -m pipelines.legislativo.ranking_politicos.ranking_senadores
