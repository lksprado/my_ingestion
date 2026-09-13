"""NHL — Times (ids) — anual.

Fonte estática: uma requisição, full refresh de ``raw_nhl.nhl_raw_*``.
"""

from core import setup_logger
from pipelines.esportes.nhl._common import run_static

logger = setup_logger(__name__)

if __name__ == "__main__":
    run_static("teams", logger)
    # uv run python -m pipelines.esportes.nhl.nhl_teams
