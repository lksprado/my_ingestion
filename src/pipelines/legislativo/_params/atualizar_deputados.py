"""Gera parameters/id_deputados.csv (coluna ``id``) para camara/camara_deputados."""

import logging
from pathlib import Path

import pandas as pd

from core import HttpClient, PipelineConfig, setup_logger

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent.parent / "camara" / "camara_config.yml"
URL = "https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome"


def obter_ids_deputados_atuais() -> None:
    data = HttpClient(logger).get_json(URL)
    if data is None:
        raise RuntimeError("Falha ao obter deputados na API da Camara.")

    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "deputados")
    pd.DataFrame(data["dados"])[["id"]].to_csv(cfg.parameter_filepath, index=False)
    logger.info(f"📄 IDs gravados em {cfg.parameter_filepath}")


if __name__ == "__main__":
    setup_logger()
    obter_ids_deputados_atuais()
    # uv run python -m pipelines.legislativo._params.atualizar_deputados
