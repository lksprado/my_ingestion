"""Parsers do HTML do e-Cidadania (consultas públicas)."""

import logging
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

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
