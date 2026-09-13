"""Orquestrador das ingestões de investimentos (falha de uma não aborta as outras).

    uv run python -m pipelines.financas.investimentos.run_all       # b3, avenue, google
    uv run python -m pipelines.financas.investimentos.run_all b3    # uma só

FGC roda à parte (depende da camada intermediate do DW):
    uv run python -m pipelines.financas.investimentos.investimentos_fgc
"""

import sys

from core import run_many
from pipelines.financas.investimentos import (
    investimentos_avenue,
    investimentos_b3,
    investimentos_google,
)

PIPELINES = {
    "b3": lambda: investimentos_b3.build().run(),
    "avenue": lambda: investimentos_avenue.build().run(),
    "google": lambda: investimentos_google.build().run(),
}

if __name__ == "__main__":
    run_many(PIPELINES, only=sys.argv[1] if len(sys.argv) > 1 else None)
