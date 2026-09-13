"""Orientações de bancada por votação, por ano (2001..)."""

import json
import logging
from pathlib import Path

import pandas as pd

from core import GenericETL, HttpClient, PipelineConfig, run_cli, write_bronze
from pipelines.legislativo._common import concat_landing, flatten_children

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"
YEARS = range(2001, 2027)
_PARENT_COLS = [
    "codigoVotacaoSve",
    "siglaTipoMateria",
    "numeroMateria",
    "anoMateria",
    "dataInicioVotacao",
    "dataTerminoVotacao",
    "descricaoVotacao",
    "qtdVotosSim",
    "qtdVotosNao",
    "qtdVotosAbstencao",
]


def extract(cfg: PipelineConfig) -> None:
    http = HttpClient(logger)
    for y in YEARS:
        data = http.get_json(f"{cfg.url_base}{y}0101/{y}1231?v=1")
        if not data:
            logger.warning(f"⚠️ Sem dados para {y}.")
            continue
        http.save_json(data, cfg.landing_dir, f"{y}_senado_votacoes_orientacao")


def _parse(path: Path) -> pd.DataFrame:
    with open(path, encoding="utf-8") as fp:
        votacoes = json.load(fp).get("votacoes", [])
    return pd.DataFrame(
        flatten_children(votacoes, _PARENT_COLS, "orientacoesLideranca")
    )


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, _parse))


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "votos_orientacao")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.senado.senado_votos_orientacao
