"""Votações do trimestre corrente (paginado); gera id_votacoes.csv e id_proposicao.csv."""

import logging
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

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
_CONFIG_FILE = Path(__file__).parent / "camara_config.yml"

QUARTERS = [
    ("01-01", "03-31", "Q1"),
    ("04-01", "06-30", "Q2"),
    ("07-01", "09-30", "Q3"),
    ("10-01", "12-31", "Q4"),
]


def _last_page(links: list) -> int:
    for link in links:
        if link.get("rel") == "last":
            qs = parse_qs(urlparse(link["href"]).query)
            return int(qs.get("pagina", [1])[0])
    return 1


def extract(cfg: PipelineConfig) -> None:
    today = date.today()
    inicio, fim, label = QUARTERS[(today.month - 1) // 3]
    y = today.year
    params = (
        f"dataInicio={y}-{inicio}&dataFim={y}-{fim}"
        "&itens=100&ordem=DESC&ordenarPor=dataHoraRegistro"
    )
    http = HttpClient(logger)
    first = http.get_json(f"{cfg.url_base}?{params}&pagina=1")
    if not first:
        logger.warning(f"⚠️ Sem dados para {y}-{label}.")
        return
    last = _last_page(first.get("links", []))
    logger.info(f"{y}-{label}: {last} pagina(s)")
    tasks = [
        (f"{cfg.url_base}?{params}&pagina={p}", f"votacoes_{y}_{label}_{p}.json")
        for p in range(1, last + 1)
    ]
    http.fetch_and_save_many(tasks, cfg.landing_dir)


def transform(cfg: PipelineConfig) -> None:
    df = concat_landing(cfg, lambda f: normalize_json_object(f, "dados"))
    if df.empty:
        write_bronze(cfg, df)
        return
    df = sanitize_columns(df)
    # id da proposição a partir da URI (.../proposicoes/2611539 -> 2611539)
    if "uriproposicaoobjeto" in df.columns:
        df["id_proposicao"] = df["uriproposicaoobjeto"].str.extract(
            r"/(\d+)/?$", expand=False
        )
    write_bronze(cfg, df)
    cfg.write_output_params(df, default_column="id")


def build() -> GenericETL:
    cfg = PipelineConfig.from_yaml(_CONFIG_FILE, "votacoes")
    return GenericETL(cfg, extract_fn=extract, transform_fn=transform, log=logger)


if __name__ == "__main__":
    run_cli(build)
    # uv run python -m pipelines.legislativo.camara.camara_votacoes
