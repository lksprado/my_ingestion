"""NHL — Boxscore (detalhes do jogo com jogadores).

Fonte dinâmica: os IDs vêm da view dbt ``staging.<param_view>`` (ver
nhl_config.yml); requer o my_datawarehouse com o seletor ``nhl`` construído.
Use ``--load-only`` para recarregar o landing sem requisitar a API.
"""

import sys

from core import setup_logger
from pipelines.esportes.nhl._common import run_dynamic

logger = setup_logger(__name__)

if __name__ == "__main__":
    run_dynamic("games_details", logger, skip_extract="--load-only" in sys.argv)
    # uv run python -m pipelines.esportes.nhl.nhl_games_details [--load-only]
