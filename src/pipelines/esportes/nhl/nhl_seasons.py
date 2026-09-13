"""NHL — Temporadas (ids) — anual.

Fonte estática: uma requisição, full refresh de ``raw.nhl_raw_*``.
"""

from core import setup_logger
from pipelines.esportes.nhl._common import run_static

logger = setup_logger(__name__)

if __name__ == "__main__":
    run_static("seasons", logger)
    # uv run python -m pipelines.esportes.nhl.nhl_seasons
