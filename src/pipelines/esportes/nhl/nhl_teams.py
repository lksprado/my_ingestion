"""NHL — Times (ids), anual. Fonte estática: uma requisição, full refresh."""

from core import run_cli
from pipelines.esportes.nhl._common import build

if __name__ == "__main__":
    run_cli(lambda: build("teams"))
    # uv run python -m pipelines.esportes.nhl.nhl_teams [--steps load]
