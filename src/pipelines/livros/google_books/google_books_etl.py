"""ETL da Google Books -> ``raw_google_books.livros``.

extract: pares (título, autor) de ``options.seed_table`` (seed do dbt, lida de
``settings.models_target``) sem JSON no landing -> busca em cascata com
conferência de autor -> um JSON por par. Grava também a lista de chaves da
seed atual em ``options.chaves_file``. transform: só os JSONs dessas chaves ->
uma linha por livro. load: full refresh (``write: truncate``).
"""

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

from core import (
    Etl,
    HttpClient,
    PipelineConfig,
    PostgresClient,
    ensure_some_success,
    normalize_string,
    read_ids,
    run_source,
    write_bronze_streaming,
    write_csv,
)
from settings import settings

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "google_books_config.yml"

# Itens por tentativa: o livro certo nem sempre é o primeiro (edições
# estrangeiras, homônimos de outro autor).
MAX_RESULTS = 5
# partial_ratio mínimo entre sobrenome da seed e autor da resposta; aceita
# variação de grafia (Tolstoi x Tolstoy x Tolstoj).
LIMIAR_AUTOR = 85
# Palavras (normalizadas) no título/subtítulo que indicam que o item não é o
# livro: resumo, análise, box com vários livros, coletânea em alemão.
TERMOS_EXCLUIDOS = {"resumo", "analise", "box", "sammlung"}


class BuscaError(Exception):
    """Requisição falhou (o HttpClient devolveu ``None``)."""


@dataclass
class Resultado:
    busca: str  # tentativa que aceitou: autor | sobrenome | titulo
    resposta: dict
    indice: int  # item aceito em resposta["items"]


def slug(texto: str) -> str:
    """Texto -> trecho de nome de arquivo (``"O Hobbit: Lá"`` -> ``o_hobbit_la``)."""
    return re.sub(r"[^a-z0-9]+", "_", normalize_string(texto)).strip("_")


def chave(titulo: str, autor: str | None) -> str:
    """Chave do landing: mudar título ou autor na seed força nova busca."""
    return f"{slug(titulo)}__{slug(autor or '')}"


def autores_seed(autor: str | None) -> list[str]:
    """Autores da seed, separados por vírgula (``"A, B"`` -> ``["A", "B"]``)."""
    return [a.strip() for a in (autor or "").split(",") if a.strip()]


def sobrenome(autor: str) -> str:
    """Última palavra do nome, normalizada (``"J.R.R. Tolkien"`` -> ``tolkien``)."""
    return slug(autor).rsplit("_", 1)[-1]


def autor_confere(autor: str | None, autores_resposta: list[str] | None) -> bool:
    """Algum sobrenome da seed aparece (fuzzy) em algum autor da resposta."""
    if not autores_resposta:
        return False
    alvos = [slug(a) for a in autores_resposta]
    return any(
        fuzz.partial_ratio(sobrenome(a), alvo) >= LIMIAR_AUTOR
        for a in autores_seed(autor)
        for alvo in alvos
    )


def tentativas(titulo: str, autor: str | None) -> list[tuple[str, str]]:
    """Consultas em ordem, da mais restrita para a mais solta: ``(busca, q)``."""
    q_titulo = f'intitle:"{titulo}"'
    autores = autores_seed(autor)
    if not autores:
        return [("titulo", q_titulo)]
    principal = autores[0]
    return [
        ("autor", f'{q_titulo} inauthor:"{principal}"'),
        ("sobrenome", f"{q_titulo} inauthor:{sobrenome(principal)}"),
        ("titulo", q_titulo),
    ]


def item_valido(item: dict) -> bool:
    """Falso se título/subtítulo tem algum termo de ``TERMOS_EXCLUIDOS``."""
    info = item.get("volumeInfo", {})
    texto = f"{info.get('title') or ''} {info.get('subtitle') or ''}"
    return not set(slug(texto).split("_")) & TERMOS_EXCLUIDOS


def em_portugues(item: dict) -> bool:
    return (item.get("volumeInfo", {}).get("language") or "").startswith("pt")


def escolher_item(items: list[dict], autor: str | None) -> int | None:
    """Índice do item aceito: válido e com autor conferido (sem autor na seed,
    qualquer válido); entre eles, o primeiro em português, senão o primeiro."""
    candidatos = [
        i
        for i, item in enumerate(items)
        if item_valido(item)
        and (
            not autores_seed(autor)
            or autor_confere(autor, item.get("volumeInfo", {}).get("authors"))
        )
    ]
    return next(
        (i for i in candidatos if em_portugues(items[i])),
        candidatos[0] if candidatos else None,
    )


def buscar_livro(
    http: HttpClient, url: str, titulo: str, autor: str | None
) -> Resultado | None:
    """Busca em cascata; ``None`` se nenhum item conferiu. Erro HTTP levanta.

    Para na primeira tentativa com item em português; item aceito em outro
    idioma só é usado se nenhuma tentativa achar um em português.
    """
    fallback = None
    for busca, q in tentativas(titulo, autor):
        # A chave vai na query string: não logue a URL; o HttpClient mascara
        # o key= nos logs de erro.
        data = http.get_json(
            url,
            params={
                "q": q,
                "maxResults": MAX_RESULTS,
                "printType": "books",
                "key": settings.google_books_api_key,
            },
        )
        if data is None:
            raise BuscaError(q)
        indice = escolher_item(data.get("items") or [], autor)
        if indice is None:
            continue
        resultado = Resultado(busca, data, indice)
        if em_portugues(data["items"][indice]):
            return resultado
        fallback = fallback or resultado
    return fallback


def parse_volume(item: dict) -> dict:
    """Um item de ``volumes`` -> dict plano com os campos de interesse."""
    info = item.get("volumeInfo", {})
    isbns = {
        i.get("type"): i.get("identifier") for i in info.get("industryIdentifiers", [])
    }
    authors = info.get("authors")
    categories = info.get("categories")
    return {
        "id": item.get("id"),
        "title": info.get("title"),
        "subtitle": info.get("subtitle"),
        "authors": ", ".join(authors) if authors else None,
        "publisher": info.get("publisher"),
        "published_date": info.get("publishedDate"),
        "page_count": info.get("pageCount"),
        "categories": ", ".join(categories) if categories else None,
        "language": info.get("language"),
        "isbn_10": isbns.get("ISBN_10"),
        "isbn_13": isbns.get("ISBN_13"),
        "description": info.get("description"),
        "thumbnail": info.get("imageLinks", {}).get("thumbnail"),
        "info_link": info.get("infoLink"),
    }


def parse_landing(content: dict) -> pd.DataFrame:
    """Um JSON do landing -> uma linha: título/autor da seed + item aceito."""
    item = content["resposta"]["items"][content["indice"]]
    return pd.DataFrame(
        [
            {
                "titulo": content["titulo"],
                "autor": content["autor"],
                "busca": content["busca"],
                **parse_volume(item),
            }
        ]
    )


def read_livros(cfg: PipelineConfig) -> list[tuple[str, str | None]]:
    """Pares (título, autor) distintos da seed, sem título vazio."""
    opts = cfg.options
    db = PostgresClient(settings.models_target, log=logger)
    df = db.read_sql(
        f'SELECT "{opts["seed_column"]}" AS titulo, '
        f'"{opts["author_column"]}" AS autor FROM {opts["seed_table"]}'
    )
    pares = []
    for titulo, autor in df.itertuples(index=False):
        titulo = str(titulo).strip() if pd.notna(titulo) else ""
        autor = str(autor).strip() if pd.notna(autor) else ""
        if titulo:
            pares.append((titulo, autor or None))
    return list(dict.fromkeys(pares))


def _landing_path(cfg: PipelineConfig, key: str) -> Path:
    return cfg.landing_dir / cfg.landing_file.format(slug=key)


def extract(cfg: PipelineConfig) -> None:
    livros = read_livros(cfg)
    chaves = pd.DataFrame({"chave": [chave(t, a) for t, a in livros]})
    write_csv(chaves, cfg.parameter_dir, cfg.options["chaves_file"], sep=",")

    pendentes = [
        (t, a) for t, a in livros if not _landing_path(cfg, chave(t, a)).exists()
    ]
    if not pendentes:
        logger.info("Nenhum livro novo na seed.")
        return
    if not settings.google_books_api_key:
        raise ValueError("Chave da Google Books ausente (GOOGLE_BOOKS_API_KEY).")

    http = HttpClient(logger, retries=3, backoff_factor=1.0, timeout=15)
    falhas = sem_resultado = 0
    for titulo, autor in pendentes:
        logger.info(f"GET volumes {titulo!r} / {autor!r}")
        try:
            resultado = buscar_livro(http, cfg.url_base, titulo, autor)
        except BuscaError:
            falhas += 1
            continue
        # Sem item com autor conferido não grava: a API às vezes responde vazio
        # para o que acha na chamada seguinte. Não conta como falha: livro que
        # nunca acha (grafia na seed) deixaria o extract vermelho todo dia.
        if resultado is None:
            logger.warning(
                f"Sem resultado com autor conferido: {titulo!r} / {autor!r}; "
                "fica pendente"
            )
            sem_resultado += 1
            continue
        http.save_json(
            {
                "titulo": titulo,
                "autor": autor,
                "busca": resultado.busca,
                "indice": resultado.indice,
                "resposta": resultado.resposta,
            },
            cfg.landing_dir,
            cfg.landing_file.format(slug=chave(titulo, autor)),
        )
    if sem_resultado:
        logger.warning(f"{sem_resultado} livro(s) sem resultado, ficam pendentes.")
    ensure_some_success(len(pendentes), falhas, "livro(s)", log=logger)


def _parse_file(path: Path) -> pd.DataFrame:
    with open(path, encoding="utf-8") as fp:
        return parse_landing(json.load(fp))


def transform(cfg: PipelineConfig) -> None:
    """Só os JSONs das chaves da seed atual: livro que saiu ou mudou na seed
    fica no landing, mas não no bronze."""
    chaves = read_ids(cfg.parameter_dir / cfg.options["chaves_file"], "chave")
    files = [p for k in chaves if (p := _landing_path(cfg, k)).exists()]
    write_bronze_streaming(cfg, files, _parse_file)


ETLS = {"livros": Etl(extract=extract, transform=transform)}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.livros.google_books.google_books_etl [--steps load]
