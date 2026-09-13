"""ETL de investimentos: uma entrada para as três origens heterogêneas.

Cada origem tem estrutura muito diferente, então o extract/transform mora num
módulo próprio; este arquivo só registra as entidades (``load: files``: cada CSV
do bronze vira ``raw_<origem>.<arquivo>``).

- ``b3``: Excel colocado manualmente no landing (``investimentos_b3``).
- ``avenue``: PDFs colocados manualmente, via pdftotext (``investimentos_avenue``).
- ``google``: Google Sheets via service account (``investimentos_google``).

O FGC fica fora (exceção ao GenericETL, depende da camada intermediate do DW):
``uv run python -m pipelines.financas.investimentos.investimentos_fgc``.
"""

from pathlib import Path

from core import Etl, run_source
from pipelines.financas.investimentos import (
    investimentos_avenue,
    investimentos_b3,
    investimentos_google,
)

CONFIG_FILE = Path(__file__).parent / "investimentos_config.yml"

ETLS = {
    "b3": Etl(transform=investimentos_b3.transform),
    "avenue": Etl(transform=investimentos_avenue.transform),
    "google": Etl(
        extract=investimentos_google.extract, transform=investimentos_google.transform
    ),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.financas.investimentos.investimentos_etl [entidade ...]
