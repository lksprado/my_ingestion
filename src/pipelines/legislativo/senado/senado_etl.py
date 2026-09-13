"""ETL do Senado Federal (Dados Abertos) -> ``raw_senado.<entidade>``.

``votacoes`` gera ``id_votacoes.csv``/``id_processo.csv`` e seu landing também
alimenta ``votos_senadores``; ``processo`` é incremental por ID; ``status`` consome
o bronze do e-Cidadania. Ordem de execução = ordem de ``ETLS``.
"""

import json
import logging
from functools import partial
from pathlib import Path

import pandas as pd

from core import (
    Etl,
    HttpClient,
    PipelineConfig,
    concat_landing,
    extract_by_ids,
    flatten_children,
    run_source,
    sanitize_columns,
    sanitize_values,
    write_bronze,
    write_bronze_streaming,
)
from core.parsers.json import normalize_json_object

logger = logging.getLogger(__name__)
CONFIG_FILE = Path(__file__).parent / "senado_config.yml"

YEARS = range(2001, 2027)

_SENADORES_KEEP_VALUES = [
    "identificacaoparlamentar_urlfotoparlamentar",
    "identificacaoparlamentar_urlpaginaparlamentar",
    "mandato_suplentes_suplente",
    "mandato_exercicios_exercicio",
    "identificacaoparlamentar_telefones_telefone",
    "identificacaoparlamentar_emailparlamentar",
    "identificacaoparlamentar_urlpaginaparticular",
]
_VOTOS_ORIENTACAO_PARENT_COLS = [
    "codigoVotacaoSve",
    "siglaTipoMateria",
    "numeroMateria",
    "anoMateria",
    "dataInicioVotacao",
    "dataTerminoVotacao",
    "descricaoVotacao",
    "qtdVotosSim",
    "qtdVotosNao",
    "qtdVotosAbstencao",
]
_VOTOS_SENADORES_PARENT_COLS = [
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


def extract_by_year(cfg: PipelineConfig, url: str, filename: str) -> None:
    """Um JSON por ano de ``YEARS``: ``url``/``filename`` com o placeholder ``{y}``."""
    http = HttpClient(logger)
    for y in YEARS:
        data = http.get_json(url.format(base=cfg.url_base, y=y))
        if not data:
            logger.warning(f"⚠️ Sem dados para {y}.")
            continue
        http.save_json(data, cfg.landing_dir, filename.format(y=y))


def _normalize_record_path(path: Path, record_path: list[str]) -> pd.DataFrame:
    with open(path, encoding="utf-8") as fp:
        return pd.json_normalize(json.load(fp), record_path=record_path, sep=".")


# ------------------------------ legislaturas / senadores ------------------------


def transform_legislaturas(cfg: PipelineConfig) -> None:
    record_path = ["ListaParlamentarLegislatura", "Parlamentares", "Parlamentar"]
    parse = partial(_normalize_record_path, record_path=record_path)
    write_bronze(cfg, concat_landing(cfg, parse))


def _parse_senadores(path: Path) -> pd.DataFrame:
    record_path = ["ListaParlamentarEmExercicio", "Parlamentares", "Parlamentar"]
    df = _normalize_record_path(path, record_path)
    return sanitize_values(sanitize_columns(df), exclude=_SENADORES_KEEP_VALUES)


def transform_senadores(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, _parse_senadores))


# ------------------------------ votacoes ------------------------------


def _parse_votacoes(path: Path) -> pd.DataFrame:
    # Array na raiz; ``votos`` (lista por linha) fica para votos_senadores.
    df = normalize_json_object(path)
    return df.drop(columns=["votos"], errors="ignore")


def transform_votacoes(cfg: PipelineConfig) -> None:
    df = sanitize_columns(concat_landing(cfg, _parse_votacoes))
    write_bronze(cfg, df)
    cfg.write_output_params(df, default_column="codigosessaovotacao")


def transform_votos_senadores(cfg: PipelineConfig) -> None:
    """Sem extract: lê o landing de ``votacoes`` (``options.source_landing_subpath``)."""
    source = cfg.landing_dir.parent / cfg.options["source_landing_subpath"]
    frames = []
    for f in sorted(source.glob("*.json")):
        try:
            with open(f, encoding="utf-8") as fp:
                rows = flatten_children(
                    json.load(fp), _VOTOS_SENADORES_PARENT_COLS, "votos"
                )
        except Exception:
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue
        if rows:
            frames.append(pd.DataFrame(rows))
    write_bronze(cfg, pd.concat(frames, ignore_index=True) if frames else None)


def _parse_votos_orientacao(path: Path) -> pd.DataFrame:
    with open(path, encoding="utf-8") as fp:
        votacoes = json.load(fp).get("votacoes", [])
    return pd.DataFrame(
        flatten_children(
            votacoes, _VOTOS_ORIENTACAO_PARENT_COLS, "orientacoesLideranca"
        )
    )


def transform_votos_orientacao(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, _parse_votos_orientacao))


# ------------------------------ status / processo ------------------------------


def extract_status(cfg: PipelineConfig) -> None:
    """Proposições do e-Cidadania com >= 5000 votos (bronze copiado para parameters)."""
    params = pd.read_csv(cfg.parameter_filepath, sep=";")
    params = params.loc[
        params["total_votos"] >= 5000, ["sigla", "numero", "ano"]
    ].drop_duplicates()

    http = HttpClient(logger)
    for _, row in params.iterrows():
        sigla, numero, ano = row["sigla"], int(row["numero"]), int(row["ano"])
        data = http.get_json(
            f"{cfg.url_base}?sigla={sigla}&numero={numero}&ano={ano}&v=1"
        )
        if data:
            http.save_json(data, cfg.landing_dir, f"status_{sigla}_{numero}_{ano}.json")


def transform_status(cfg: PipelineConfig) -> None:
    write_bronze(cfg, concat_landing(cfg, pd.read_json))


def _parse_processo(path: Path) -> pd.DataFrame | None:
    """Array na raiz; ``id_processo`` (id consultado) vem do nome do arquivo."""
    with open(path, encoding="utf-8") as fp:
        raw = json.load(fp)
    if not raw:
        return None
    df = pd.json_normalize(raw, sep=".")
    df["id_processo"] = path.stem.removesuffix("_processo")
    return df


def transform_processo(cfg: PipelineConfig) -> None:
    write_bronze_streaming(cfg, sorted(cfg.landing_dir.glob("*.json")), _parse_processo)


ETLS = {
    "legislaturas": Etl(transform=transform_legislaturas),
    "senadores": Etl(transform=transform_senadores),
    "votacoes": Etl(
        extract=partial(
            extract_by_year,
            url="{base}?dataInicio={y}-01-01&dataFim={y}-12-31&v=1",
            filename="{y}_senado_votacoes",
        ),
        transform=transform_votacoes,
    ),
    "votos_senadores": Etl(transform=transform_votos_senadores),
    "votos_orientacao": Etl(
        extract=partial(
            extract_by_year,
            url="{base}{y}0101/{y}1231?v=1",
            filename="{y}_senado_votacoes_orientacao",
        ),
        transform=transform_votos_orientacao,
    ),
    "processo": Etl(
        extract=extract_by_ids,  # array na raiz: qualquer resposta não vazia é dado
        transform=transform_processo,
    ),
    "status": Etl(extract=extract_status, transform=transform_status),
}

if __name__ == "__main__":
    run_source(CONFIG_FILE, ETLS)
    # uv run python -m pipelines.legislativo.senado.senado_etl [entidade ...] [--steps ...]
