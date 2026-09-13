"""Ranking dos Políticos: deputados."""

from core import run_cli
from pipelines.legislativo.ranking_politicos._common import build

if __name__ == "__main__":
    run_cli(lambda: build("deputados"))
    # uv run python -m pipelines.legislativo.ranking_politicos.ranking_deputados
