"""Transform do governismo do Radar Congresso (wide trimestral -> long)."""

import json
import re

import pandas as pd

from core import PipelineConfig, write_bronze

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
