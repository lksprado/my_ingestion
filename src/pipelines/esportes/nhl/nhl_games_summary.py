"""NHL — Resumo de todos os jogos: base dos IDs dos demais. Fonte estática."""

from core import run_cli
from pipelines.esportes.nhl._common import build

if __name__ == "__main__":
    run_cli(lambda: build("games_summary"))
    # uv run python -m pipelines.esportes.nhl.nhl_games_summary [--steps load]
