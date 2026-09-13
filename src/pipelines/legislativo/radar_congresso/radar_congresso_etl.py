"""ETL do Radar Congresso (Congresso em Foco) -> ``raw_radar_congresso.<entidade>``.

Extract padrão da core (``base_url`` -> ``landing_file``). O governismo vem wide
(uma coluna por trimestre) e vira long no transform.
"""

import json
import logging
import re
from pathlib import Path

import pandas as pd

from core import Etl, PipelineConfig, run_source, write_bronze

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "radar_congresso_config.yml"

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def transform_governismo(cfg: PipelineConfig) -> None:
    with cfg.landing_filepath.open(encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.json_normalize(pd.DataFrame(raw)["parlamentares"])
    df = df.dropna(subset=["id"]).reset_index(drop=True)
    df = df[[c for c in df.columns if "trimestral" not in c or "total" in c]]

    # Colunas trimestrais viram "YYYY-MM-DD"; sem data no nome, a coluna é descartada.
    renames = {}
    for c in df.columns:
        if "trimestral" in c:
            m = _DATE_RE.search(c)
            if m:
                renames[c] = m.group(0)
    df = df.rename(columns=renames)

    df_long = df.melt(
        id_vars=["id", "afavor", "n", "total"],
        value_vars=list(renames.values()),
        var_name="trimestre",
        value_name="perc_governismo",
    )
    write_bronze(cfg, df_long)


def transform_parlamentares(cfg: PipelineConfig) -> None:
    write_bronze(cfg, pd.read_json(cfg.landing_filepath, dtype=str))


ETLS = {
    "governismo_deputados": Etl(transform=transform_governismo),
    "governismo_senadores": Etl(transform=transform_governismo),
    "parlamentares": Etl(transform=transform_parlamentares),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.legislativo.radar_congresso.radar_congresso_etl [entidade ...] [--steps ...]
