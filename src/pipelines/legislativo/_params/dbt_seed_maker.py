"""Tabelas de domínio do Senado como seeds do dbt (my_analytics).

São listas de referência pequenas e quase estáticas (siglas, tipos de decisão,
entes) que o dbt lê por ``ref('seed_senado_tipos_*')``: não passam pela raw nem
pelo ``GenericETL``, viram CSV direto em ``settings.seeds_root``.

O arquivo é sobrescrito, e a API do Senado às vezes **encolhe** (uma sigla sai
da lista sem que nada a substitua). Como o seed é a fonte de verdade do join no
dbt, perder linha é perder descrição em dado histórico — por isso, quando o
novo conteúdo tem menos linhas que o seed atual, sai um WARNING dizendo quantas
e o arquivo **não** é gravado. ``--forcar`` grava mesmo assim.
"""

import logging
from pathlib import Path

import pandas as pd

from core import HttpClient, setup_logger
from settings import settings

logger = logging.getLogger(__name__)

SEEDS = {
    "seed_senado_tipos_entes": "https://legis.senado.leg.br/dadosabertos/processo/entes",
    "seed_senado_tipos_decisao": "https://legis.senado.leg.br/dadosabertos/processo/tipos-decisao",
    "seed_senado_tipos_projetos": "https://legis.senado.leg.br/dadosabertos/processo/siglas",
}


def _linhas_atuais(destino: Path) -> int | None:
    """Quantas linhas o seed já tem; ``None`` quando ainda não existe."""
    if not destino.exists():
        return None
    return sum(1 for _ in destino.open(encoding="utf-8")) - 1  # menos o cabeçalho


def gerar_seed(nome: str, forcar: bool = False) -> Path | None:
    data = HttpClient(logger).get_json(SEEDS[nome])
    if data is None:
        raise RuntimeError(f"Falha ao obter {nome}.")
    df = pd.DataFrame(data)

    destino = settings.seeds_root / f"{nome}.csv"
    atuais = _linhas_atuais(destino)
    if atuais is not None and len(df) < atuais and not forcar:
        logger.warning(
            f"⚠️ {nome}: a API devolveu {len(df)} linha(s) e o seed tem {atuais}; "
            "não gravei. Confira o que saiu da lista e use --forcar se for esperado."
        )
        return None

    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destino, sep=",", index=False)  # seed do dbt: ","
    logger.info(f"📄 Seed gravada em {destino} ({len(df)} linha(s))")
    return destino


if __name__ == "__main__":
    import sys

    setup_logger()
    argv = sys.argv[1:]
    forcar = "--forcar" in argv
    nomes = [a for a in argv if not a.startswith("--")] or list(SEEDS)
    desconhecidos = [n for n in nomes if n not in SEEDS]
    if desconhecidos:
        raise SystemExit(
            f"seed(s) desconhecida(s) {desconhecidos}; opções: {list(SEEDS)}"
        )
    for nome in nomes:
        gerar_seed(nome, forcar=forcar)
    # uv run python -m pipelines.legislativo._params.dbt_seed_maker [nome ...] [--forcar]
