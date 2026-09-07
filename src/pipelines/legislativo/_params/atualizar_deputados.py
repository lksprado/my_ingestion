"""Gera o CSV de IDs de deputados consumido pelo pipeline camara/deputados."""

from pathlib import Path

import pandas as pd

from core import PipelineConfig, load_source_config
from core.http import HttpClient

_CONFIG_FILE = Path(__file__).parent.parent / "camara" / "camara_config.yml"


def obter_ids_deputados_atuais():
    """Obtem todos os deputados atuais e grava id_deputados.csv em parameter_dir."""
    extractor = HttpClient()
    data = extractor.make_http_request(
        url="https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome",
    )
    if data is None:
        raise RuntimeError("Falha ao obter deputados na API da Camara.")

    df = pd.DataFrame(data["dados"])

    cfg = PipelineConfig(**load_source_config(_CONFIG_FILE, source="deputados"))
    df.to_csv(cfg.parameter_filepath, sep=";")
    print(f"Finalizado: {cfg.parameter_filepath}")


if __name__ == "__main__":
    obter_ids_deputados_atuais()
    # python -m pipelines.legislativo._params.atualizar_deputados
