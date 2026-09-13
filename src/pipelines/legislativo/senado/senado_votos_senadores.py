"""Votos individuais por votação; lê o landing de ``votacoes`` (sem extract próprio)."""

import json
import logging
from pathlib import Path

import pandas as pd

from core import GenericETL, PipelineConfig, run_cli, write_bronze
from pipelines.legislativo._common import flatten_children

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"
_PARENT_COLS = [
    "ano",
    "casaSessao",
    "codigoMateria",
    "codigoSessao",
    "codigoSessaoLegislativa",
    "codigoSessaoVotacao",
    "codigoVotacaoSve",
    "dataApresentacao",
    "dataSessao",
    "idProcesso",
    "identificacao",
    "numero",
    "numeroSessao",
    "resultadoVotacao",
    "sequencialSessao",
    "sigla",
    "siglaTipoSessao",
    "totalVotosAbstencao",
    "totalVotosNao",
    "totalVotosSim",
    "votacaoSecreta",
]


def transform(cfg: PipelineConfig) -> None:
    source = cfg.landing_dir.parent / cfg.options["source_landing_subpath"]
    frames = []
    for f in sorted(source.glob("*.json")):
        try:
            with open(f, encoding="utf-8") as fp:
                rows = flatten_children(json.load(fp), _PARENT_COLS, "votos")
        except Exception:
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue
        if rows:
            frames.append(pd.DataFrame(rows))
    write_bronze(cfg, pd.concat(frames, ignore_index=True) if frames else None)


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "votos_senadores")
    return GenericETL(cfg, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.senado.senado_votos_senadores
