"""Votações nominais por ano (2001..); gera id_votacoes.csv e id_processo.csv."""

import logging
from pathlib import Path

from core import (
    GenericETL,
    HttpClient,
    PipelineConfig,
    run_cli,
    sanitize_columns,
    write_bronze,
)
from core.parsers.json import normalize_json_object
from pipelines.legislativo._common import concat_landing

logger = logging.getLogger(__name__)
_CONFIG_FILE = Path(__file__).parent / "senado_config.yml"
YEARS = range(2001, 2027)


def extract(cfg: PipelineConfig) -> None:
    http = HttpClient(logger)
    for y in YEARS:
        data = http.get_json(
            f"{cfg.url_base}?dataInicio={y}-01-01&dataFim={y}-12-31&v=1"
        )
        if not data:
            logger.warning(f"⚠️ Sem dados para {y}.")
            continue
        http.save_json(data, cfg.landing_dir, f"{y}_senado_votacoes")


def _parse(path: Path):
    # Array na raiz; ``votos`` (lista por linha) fica para senado_votos_senadores.
    df = normalize_json_object(path)
    return df.drop(columns=["votos"], errors="ignore")


def transform(cfg: PipelineConfig) -> None:
    df = sanitize_columns(concat_landing(cfg, _parse))
    write_bronze(cfg, df)
    cfg.write_output_params(df, default_column="codigosessaovotacao")


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "votacoes")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.senado.senado_votacoes
