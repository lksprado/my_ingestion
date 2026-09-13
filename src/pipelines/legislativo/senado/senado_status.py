"""Status das proposições mais votadas no e-Cidadania (>= 5000 votos)."""

import logging
from pathlib import Path

import pandas as pd

from core import GenericETL, HttpClient, PipelineConfig, run_cli, write_bronze
from pipelines.legislativo._common import concat_landing

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"


def extract(cfg: PipelineConfig) -> None:
    params = pd.read_csv(cfg.parameter_filepath, sep=";")
    params = params.loc[
        params["total_votos"] >= 5000, ["sigla", "numero", "ano"]
    ].drop_duplicates()

    http = HttpClient(logger)
    for _, row in params.iterrows():
        sigla, numero, ano = row["sigla"], int(row["numero"]), int(row["ano"])
        data = http.get_json(
            f"{cfg.url_base}?sigla={sigla}&numero={numero}&ano={ano}&v=1"
        )
        if data:
            http.save_json(data, cfg.landing_dir, f"status_{sigla}_{numero}_{ano}.json")


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, pd.read_json))


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "status")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.senado.senado_status
