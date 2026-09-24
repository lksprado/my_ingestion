"""ETL da Câmara dos Deputados (Dados Abertos) -> ``raw_camara.<entidade>``.

Envelope da API: ``{"dados": [...], "links": [...]}``. As entidades por ID
(``{id}`` em ``base_url``/``landing_file``) usam ``core.extract_by_ids`` e
consomem os parâmetros que ``votacoes`` gera. As ``arquivo_*`` baixam os arquivos
anuais (``{ano}``), que têm o mesmo envelope. Ordem de execução = ordem de ``ETLS``.
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
    landing_ids,
    pending_ids,
    read_ids,
    run_source,
    sanitize_columns,
    sanitize_values,
    write_bronze,
    write_bronze_incremental,
    write_bronze_streaming,
)
from core.parsers.json import normalize_json_object

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "camara_config.yml"

QUARTERS = ["01-01", "04-01", "07-01", "10-01"]

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
    """Para landings com milhares de arquivos (entidades por ID).

    Com ``options.control_table`` no YAML o bronze sai só com os arquivos ainda
    não carregados (e um manifesto ao lado); sem ela, é o rebuild completo.
    """
    write_bronze_incremental(
        cfg,
        sorted(cfg.landing_dir.glob("*.json")),
        partial(parse_dados_abertos, url_col=url_col),
        log=logger,
    )


# ------------------------------ legislaturas ------------------------------


def extract_legislaturas(cfg: PipelineConfig) -> None:
    """Deputados de cada legislatura de ``options.legislaturas``, todas as páginas.

    Grava um JSON por legislatura com as páginas juntas no envelope da API, o que
    sobrescreve o arquivo anterior sem duplicar. Se alguma página falhar, a
    legislatura é pulada e o arquivo anterior fica.
    """
    http = HttpClient(logger)
    for leg in cfg.options["legislaturas"]:
        url = cfg.url_base.format(legislatura=leg)
        first = http.get_json(f"{url}&pagina=1")
        if not first:
            logger.warning(f"⚠️ Sem resposta para a legislatura {leg}.")
            continue
        last = _last_page(first.get("links", []))
        pages = [first] + [
            http.get_json(f"{url}&pagina={p}") for p in range(2, last + 1)
        ]
        if any(page is None for page in pages):
            logger.warning(
                f"⚠️ Legislatura {leg} incompleta; mantido o arquivo anterior."
            )
            continue
        data = {
            "dados": [d for page in pages for d in page.get("dados") or []],
            "links": [{"rel": "self", "href": url}],
        }
        logger.info(f"Legislatura {leg}: {len(data['dados'])} deputado(s)")
        http.save_json(data, cfg.landing_dir, cfg.landing_file.format(legislatura=leg))


def transform_legislaturas(cfg: PipelineConfig) -> None:
    """Bronze + ``id_deputados_legislaturas.csv`` para ``deputados``."""
    df = concat_landing(cfg, partial(parse_dados_abertos, url_col="url_link"))
    write_bronze(cfg, df)
    if not df.empty:
        cfg.write_output_params(df, default_column="id")


# ------------------------------ deputados ------------------------------


def deputados_a_baixar(cfg: PipelineConfig) -> list[str]:
    """IDs cuja ficha será baixada.

    Os de ``id_deputados.csv`` (atuais) sempre, porque a ficha muda. Os de
    ``options.historico_file`` (todas as legislaturas, gerado por
    ``legislaturas``) só quando ainda não estão no landing.
    """
    atuais = read_ids(cfg.parameter_filepath, "id")
    historico = cfg.parameter_dir / cfg.options.get("historico_file", "")
    if not cfg.options.get("historico_file") or not historico.is_file():
        logger.warning(f"⚠️ Sem {historico.name}: só os deputados atuais.")
        return atuais
    suffix = Path(cfg.landing_file.replace("{id}", "")).stem
    baixados = landing_ids(cfg.landing_dir, suffix) | set(atuais)
    return atuais + pending_ids(read_ids(historico, "id"), baixados)


def extract_deputados(cfg: PipelineConfig) -> None:
    """Ficha de cada deputado de ``deputados_a_baixar`` (full refresh)."""
    ids = deputados_a_baixar(cfg)
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


def trimestres(
    ultimos: int = 1, desde: int | None = None, hoje: date | None = None
) -> list[tuple[int, str, str, str]]:
    """``(ano, dataInicio, dataFim, rótulo)`` do mais antigo ao trimestre corrente.

    Com ``desde`` (ano), todos os trimestres desde janeiro dele; sem ele, os
    ``ultimos`` trimestres (a API recusa intervalo maior que 3 meses).
    ``dataFim`` é o 1º dia do trimestre seguinte: na API ele é exclusivo, e
    terminar no último dia do trimestre perdia o que foi registrado nesse dia.
    """
    hoje = hoje or date.today()
    atual = hoje.year * 4 + (hoje.month - 1) // 3
    primeiro = desde * 4 if desde is not None else atual - ultimos + 1

    def inicio(i: int) -> str:
        return f"{i // 4}-{QUARTERS[i % 4]}"

    return [
        (i // 4, inicio(i), inicio(i + 1), f"Q{i % 4 + 1}")
        for i in range(primeiro, atual + 1)
    ]


def extract_votacoes(cfg: PipelineConfig, desde: int | None = None) -> None:
    """Votações dos últimos ``options.trimestres`` trimestres, todas as páginas.

    Rebaixar o trimestre anterior pega o que foi registrado depois da última
    execução dele (a DAG é semanal). ``desde`` (ano) é o backfill de
    ``scripts/camara_votacoes_backfill.py``. Os arquivos têm nome fixo por
    trimestre e página, então rebaixar sobrescreve.
    """
    http = HttpClient(logger)
    for y, inicio, fim, label in trimestres(
        int(cfg.options.get("trimestres", 1)), desde
    ):
        params = (
            f"dataInicio={inicio}&dataFim={fim}"
            "&itens=100&ordem=DESC&ordenarPor=dataHoraRegistro"
        )
        first = http.get_json(f"{cfg.url_base}?{params}&pagina=1")
        if not first:
            logger.warning(f"⚠️ Sem dados para {y}-{label}.")
            continue
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
    # A votação registrada à 0h do 1º dia do trimestre cai nas duas janelas.
    df = sanitize_columns(df).drop_duplicates(subset="id")
    # id da proposição a partir da URI (.../proposicoes/2611539 -> 2611539)
    if "uriproposicaoobjeto" in df.columns:
        df["id_proposicao"] = df["uriproposicaoobjeto"].str.extract(
            r"/(\d+)/?$", expand=False
        )
    write_bronze(cfg, df)
    cfg.write_output_params(df, default_column="id")


# ------------------------------ arquivos anuais ------------------------------


def extract_arquivo_anual(cfg: PipelineConfig) -> None:
    """Um arquivo por ano (``{ano}``), de ``options.ano_inicio`` ao ano corrente.

    Todos são rebaixados: o arquivo de um ano traz o status atual das
    proposições dele. Ano sem arquivo (404) só é logado.
    """
    anos = range(int(cfg.options["ano_inicio"]), date.today().year + 1)
    tasks = [(cfg.url_base.format(ano=a), cfg.landing_file.format(ano=a)) for a in anos]
    # Arquivos de ~100 MB: o timeout padrão (30 s) não basta.
    HttpClient(logger, timeout=300).fetch_and_save_many(
        tasks, cfg.landing_dir, workers=int(cfg.options.get("workers", 1))
    )


def parse_arquivo_anual(path: Path) -> pd.DataFrame | None:
    """``dados`` achatado; ``None`` se o arquivo não tem registros."""
    dados, _ = read_dados_abertos(path)
    return pd.json_normalize(dados, sep=".") if dados else None


def transform_arquivo_anual(cfg: PipelineConfig) -> None:
    # Ano mais recente primeiro: o streaming fixa o cabeçalho pelo 1º arquivo e
    # descarta colunas extras dos seguintes, e o esquema novo é o mais completo.
    arquivos = sorted(cfg.landing_dir.glob("*.json"), reverse=True)
    write_bronze_streaming(cfg, arquivos, parse_arquivo_anual)


ETLS = {
    "legislaturas": Etl(extract=extract_legislaturas, transform=transform_legislaturas),
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
    "arquivo_proposicoes": Etl(
        extract=extract_arquivo_anual, transform=transform_arquivo_anual
    ),
    "arquivo_proposicoes_temas": Etl(
        extract=extract_arquivo_anual, transform=transform_arquivo_anual
    ),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.legislativo.camara.camara_etl [entidade ...] [--steps ...]
