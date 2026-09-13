"""ETL da Câmara dos Deputados (Dados Abertos) -> ``raw_camara.<entidade>``.

Envelope da API: ``{"dados": [...], "links": [...]}``. As entidades por ID
(``{id}`` em ``base_url``/``landing_file``) usam ``core.extract_by_ids`` e
consomem os parâmetros que ``votacoes`` gera. Ordem de execução = ordem de ``ETLS``.
"""

import json
import logging
from datetime import date
from functools import partial
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd

from core import (
    Etl,
    HttpClient,
    PipelineConfig,
    concat_landing,
    extract_by_ids,
    read_ids,
    run_source,
    sanitize_columns,
    sanitize_values,
    write_bronze,
    write_bronze_streaming,
)
from core.parsers.json import normalize_json_object

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "camara_config.yml"

QUARTERS = [
    ("01-01", "03-31", "Q1"),
    ("04-01", "06-30", "Q2"),
    ("07-01", "09-30", "Q3"),
    ("10-01", "12-31", "Q4"),
]

# Colunas de deputados cujos valores não passam por sanitize_values (URLs, e-mails).
_DEPUTADOS_KEEP_VALUES = [
    "uri",
    "urlwebsite",
    "redesocial",
    "datanascimento",
    "datafalecimento",
    "ultimostatus_uri",
    "ultimostatus_uripartido",
    "ultimostatus_urlfoto",
    "ultimostatus_email",
    "ultimostatus_data",
    "ultimostatus_gabinete_telefone",
    "ultimostatus_gabinete_email",
]


# ------------------------------ Dados Abertos ------------------------------


def read_dados_abertos(path: Path) -> tuple[list, str | None]:
    """``(dados, href do link rel=self)`` de um JSON da API da Câmara."""
    with open(path, encoding="utf-8") as fp:
        raw = json.load(fp)
    dados = raw.get("dados") or []
    self_url = next(
        (lnk["href"] for lnk in raw.get("links", []) if lnk.get("rel") == "self"),
        None,
    )
    return dados, self_url


def parse_dados_abertos(path: Path, url_col: str) -> pd.DataFrame | None:
    """DataFrame achatado de ``dados`` com a URL da requisição em ``url_col``.

    ``None`` se o arquivo não tem registros.
    """
    dados, self_url = read_dados_abertos(path)
    if not dados:
        return None
    df = pd.json_normalize(dados, sep=".")
    df[url_col] = self_url
    return df


def _has_dados(data) -> bool:
    return bool(data and data.get("dados"))


extract_ids = partial(extract_by_ids, has_data=_has_dados)


def transform_dados_abertos(cfg: PipelineConfig, url_col: str) -> None:
    write_bronze(
        cfg, concat_landing(cfg, partial(parse_dados_abertos, url_col=url_col))
    )


def transform_dados_abertos_streaming(cfg: PipelineConfig, url_col: str) -> None:
    """Para landings com milhares de arquivos (entidades por ID)."""
    write_bronze_streaming(
        cfg,
        sorted(cfg.landing_dir.glob("*.json")),
        partial(parse_dados_abertos, url_col=url_col),
    )


# ------------------------------ deputados ------------------------------


def extract_deputados(cfg: PipelineConfig) -> None:
    """Ficha de cada deputado de ``id_deputados.csv`` (full refresh, sem incremental)."""
    ids = read_ids(cfg.parameter_filepath, "id")
    tasks = [(cfg.url_base.format(id=i), cfg.landing_file.format(id=i)) for i in ids]
    HttpClient(logger).fetch_and_save_many(
        tasks, cfg.landing_dir, workers=int(cfg.options.get("workers", 1))
    )


def _parse_deputado(path: Path) -> pd.DataFrame | None:
    df = normalize_json_object(path, "dados")
    if df.empty:
        return None
    return sanitize_values(sanitize_columns(df), exclude=_DEPUTADOS_KEEP_VALUES)


def transform_deputados(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, _parse_deputado))


# ------------------------------ votacoes ------------------------------


def _last_page(links: list) -> int:
    for link in links:
        if link.get("rel") == "last":
            qs = parse_qs(urlparse(link["href"]).query)
            return int(qs.get("pagina", [1])[0])
    return 1


def extract_votacoes(cfg: PipelineConfig) -> None:
    """Votações do trimestre corrente, todas as páginas."""
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


def transform_votacoes(cfg: PipelineConfig) -> None:
    """Bronze + ``id_votacoes.csv``/``id_proposicao.csv`` para as entidades por ID."""
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


ETLS = {
    "legislaturas": Etl(
        transform=partial(transform_dados_abertos, url_col="url_link"),
    ),
    "deputados": Etl(extract=extract_deputados, transform=transform_deputados),
    "votacoes": Etl(extract=extract_votacoes, transform=transform_votacoes),
    "votos_deputados": Etl(
        extract=extract_ids,
        transform=partial(transform_dados_abertos_streaming, url_col="url_votos"),
    ),
    "votos_orientacao": Etl(
        extract=extract_ids,
        transform=partial(transform_dados_abertos, url_col="url_votos"),
    ),
    "proposicao_tema": Etl(
        extract=extract_ids,
        transform=partial(transform_dados_abertos_streaming, url_col="url_temas"),
    ),
    "proposicao": Etl(
        extract=extract_ids,
        transform=partial(transform_dados_abertos_streaming, url_col="urls"),
    ),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.legislativo.camara.camara_etl [entidade ...] [--steps ...]
