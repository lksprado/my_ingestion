"""ETL do e-Cidadania (consultas públicas do Senado) -> ``raw_ecidadania.<entidade>``.

Scraping do HTML: o extract já grava no landing o CSV parseado (``;``), não o HTML
bruto — reprocessar exige nova extração. O transform concatena os CSVs do landing.
O bronze de ``paginas`` é parâmetro do ``senado_etl status``.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from functools import partial
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from core import (
    Etl,
    HttpClient,
    PipelineConfig,
    concat_landing,
    run_source,
    write_bronze,
)
from core.parsers.html import make_bs_object

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "ecidadania_config.yml"

SITE = "https://www12.senado.leg.br/ecidadania/"

TIPO_MAP = {
    "ECD": "EMENDA(S) DA CÂMARA DOS DEPUTADOS A PROJETO DE LEI DO SENADO",
    "EDS": "EMENDA(S) DA CÂMARA DOS DEPUTADOS A PROJETO DE DECRETO LEGISLATIVO",
    "MPV": "MEDIDA PROVISÓRIA",
    "PDL": "PROJETO DE DECRETO LEGISLATIVO",
    "PDS": "PROJETO DE DECRETO LEGISLATIVO (SF)",
    "PEC": "PROPOSTA DE EMENDA À CONSTITUIÇÃO",
    "PL": "PROJETO DE LEI",
    "PLC": "PROJETO DE LEI DA CÂMARA",
    "PLP": "PROJETO DE LEI COMPLEMENTAR",
    "PLS": "PROJETO DE LEI DO SENADO",
    "PLV": "PROJETO DE LEI DE CONVERSÃO (CN)",
    "PRS": "PROJETO DE RESOLUÇÃO DO SENADO",
    "SCD": "SUBSTITUTIVO DA CÂMARA DOS DEPUTADOS A PROJETO DE LEI DO SENADO",
    "SDS": "SUBSTITUTIVO DA CÂMARA DOS DEPUTADOS A PROJETO DE DECRETO LEGISLATIVO",
    "SUG": "SUGESTÃO",
}


def _today() -> str:
    return datetime.today().strftime("%Y-%m-%d")


def _int(text: str) -> int:
    return int(text.replace(".", "").strip())


def parse_materias(soup: BeautifulSoup) -> pd.DataFrame:
    """Uma linha por matéria listada (usado por ``paginas`` e ``mais_votados``)."""
    container = soup.find("div", id="container-consulta-publica")
    if not container:
        logger.warning("⚠️ container-consulta-publica nao encontrado; DataFrame vazio.")
        return pd.DataFrame()

    dt_extracao = _today()
    results = []
    for item in container.find_all("div", class_="resumo-materia"):
        header = item.find("header")
        section = item.find("section")
        figure = item.find("figure", class_="grafico-consulta-publica")

        titulo_tag = header.find("a") if header else None
        descritivo_tag = section.find("a") if section else None
        titulo = titulo_tag.get_text(strip=True) if titulo_tag else None
        if not titulo:
            continue
        descritivo = descritivo_tag.get_text(strip=True) if descritivo_tag else None
        href = titulo_tag.get("href")
        sigla, resto = (titulo.split(" ", 1) + [""])[:2]
        numero, _, ano = resto.partition("/")

        votos_sim = votos_nao = 0
        if figure:
            spans = figure.select("header span")
            if len(spans) >= 2:
                try:
                    votos_sim, votos_nao = _int(spans[0].text), _int(spans[1].text)
                except ValueError:
                    pass

        results.append(
            {
                "dt_extracao": dt_extracao,
                "sigla": sigla,
                "numero": numero,
                "ano": ano,
                "titulo": titulo,
                "tipo_proposicao": TIPO_MAP.get(sigla, "DESCONHECIDO"),
                "descritivo": descritivo,
                "votos_sim": votos_sim,
                "votos_nao": votos_nao,
                "link": f"{SITE}{href.lstrip('/')}" if href else None,
            }
        )
    return pd.DataFrame(results)


def parse_big_numbers(soup: BeautifulSoup) -> pd.DataFrame:
    """Os três contadores da página principal (uma linha)."""
    container = soup.find("div", id="container-consulta-publica")
    boxes = container.select("div.box-est-cp > header") if container else []
    if len(boxes) < 3:
        logger.warning("⚠️ Contadores nao encontrados; DataFrame vazio.")
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "total_proposicoes_votadas": _int(boxes[0].text),
                "total_pessoas_votaram": _int(boxes[1].text),
                "total_votos_registrados": _int(boxes[2].text),
                "dt_extracao": _today(),
            }
        ]
    )


# ------------------------------ extract / transform ------------------------------


def fetch_to_csv(
    cfg: PipelineConfig,
    parser: Callable[[BeautifulSoup], pd.DataFrame],
    url: str,
    filename: str,
    http: HttpClient | None = None,
) -> None:
    """Baixa ``url``, parseia e grava o CSV (``;``) em ``landing_dir/filename``."""
    html = (http or HttpClient(logger)).get_text(url)
    if html is None:
        logger.warning(f"⚠️ Sem resposta de {url}")
        return
    df = parser(make_bs_object(response=html))
    df.to_csv(cfg.landing_dir / filename, sep=";", index=False)


def extract_page(
    cfg: PipelineConfig, parser: Callable[[BeautifulSoup], pd.DataFrame]
) -> None:
    """Uma página (``base_url``) -> ``landing_file``."""
    fetch_to_csv(cfg, parser, cfg.url_base, cfg.landing_file)


def extract_paginas(cfg: PipelineConfig) -> None:
    """Todas as páginas de consultas públicas (``options.pages``)."""
    http = HttpClient(logger)
    for page in range(1, int(cfg.options.get("pages", 145)) + 1):
        logger.info(f"Extraindo pagina {page}...")
        fetch_to_csv(
            cfg,
            parse_materias,
            f"{cfg.url_base}{page}",
            cfg.landing_file.format(page=page),
            http=http,
        )


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", dtype=str)


def _concat_csvs(cfg: PipelineConfig) -> pd.DataFrame:
    return concat_landing(cfg, _read_csv, pattern="*.csv")


def transform(cfg: PipelineConfig) -> None:
    write_bronze(cfg, _concat_csvs(cfg))


def transform_paginas(cfg: PipelineConfig) -> None:
    """Como ``transform``, acrescentando ``total_votos`` (sim + não)."""
    df = _concat_csvs(cfg)
    if not df.empty:
        df["total_votos"] = pd.to_numeric(df["votos_sim"], errors="coerce") + (
            pd.to_numeric(df["votos_nao"], errors="coerce")
        )
    write_bronze(cfg, df)


ETLS = {
    "bignumbers": Etl(
        extract=partial(extract_page, parser=parse_big_numbers), transform=transform
    ),
    "mais_votados": Etl(
        extract=partial(extract_page, parser=parse_materias), transform=transform
    ),
    "paginas": Etl(extract=extract_paginas, transform=transform_paginas),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.legislativo.ecidadania.ecidadania_etl [entidade ...] [--steps ...]
