"""Helpers compartilhados pelos pipelines do domínio legislativo.

- Envelope dos Dados Abertos da Câmara (``{"dados": [...], "links": [...]}``).
- Extração incremental por ID dirigida pelo YAML: ``base_url`` e ``landing_file``
  com o placeholder ``{id}``, ``parameter_file`` com os IDs e, em ``options``,
  ``no_data_file`` (CSV dos IDs que a API nunca respondeu), ``parameter_column``
  (default ``id``) e ``blacklist_on_error`` (default ``true``).
- Transform "um DataFrame por arquivo do landing → concat".
"""

import json
import logging
from collections.abc import Callable, Sequence
from pathlib import Path

import pandas as pd

from core import HttpClient, PipelineConfig, mark_no_data, pending_ids

logger = logging.getLogger(__name__)


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


# ------------------------------ extração por ID ------------------------------


def read_ids(path: Path, column: str) -> list[str]:
    """IDs únicos de uma coluna de CSV, como str (sem ``.0`` de float)."""
    s = pd.read_csv(path)[column].dropna()
    if pd.api.types.is_numeric_dtype(s):
        s = s.astype("int64")
    return s.astype(str).drop_duplicates().tolist()


def _suffix(landing_template: str) -> str:
    """``"{id}_votos.json"`` -> ``"_votos"`` (para recuperar o id do nome)."""
    return Path(landing_template.replace("{id}", "")).stem


def landing_ids(landing_dir: Path, suffix: str) -> set[str]:
    """IDs já baixados: stems dos ``.json`` do landing sem o sufixo."""
    return {f.stem.removesuffix(suffix) for f in landing_dir.glob("*.json")}


def _has_dados(data) -> bool:
    return bool(data and data.get("dados"))


def extract_by_ids(
    cfg: PipelineConfig,
    has_data: Callable[[object], bool] = _has_dados,
    http: HttpClient | None = None,
) -> None:
    """Requisita ``cfg.url_base.format(id=...)`` para cada ID pendente.

    Pendente = ``parameter_file`` menos os já no landing e os do ``no_data_file``.
    Resposta sem dados vai para o ``no_data_file``; erro/timeout também, salvo
    ``options.blacklist_on_error: false``.
    """
    opts = cfg.options
    no_data = cfg.parameter_dir / opts["no_data_file"]
    suffix = _suffix(cfg.landing_file)
    ids = read_ids(cfg.parameter_filepath, opts.get("parameter_column", "id"))
    todo = pending_ids(ids, landing_ids(cfg.landing_dir, suffix), no_data)
    blacklist_on_error = bool(opts.get("blacklist_on_error", True))
    http = http or HttpClient(logger)

    for id_ in todo:
        data = http.get_json(cfg.url_base.format(id=id_))
        if has_data(data):
            http.save_json(data, cfg.landing_dir, cfg.landing_file.format(id=id_))
        elif data is None:
            logger.warning(f"⚠️ Erro/timeout para {id_}.")
            if blacklist_on_error:
                mark_no_data(no_data, id_)
        else:
            logger.warning(f"⚠️ Sem dados para {id_}; registrado em {no_data.name}.")
            mark_no_data(no_data, id_)


# ------------------------------ transform ------------------------------


def concat_landing(
    cfg: PipelineConfig,
    parse_fn: Callable[[Path], pd.DataFrame | None],
    pattern: str = "*.json",
) -> pd.DataFrame:
    """Aplica ``parse_fn`` a cada arquivo do landing e concatena (erro pula o arquivo)."""
    frames = []
    for f in sorted(cfg.landing_dir.glob(pattern)):
        try:
            df = parse_fn(f)
        except Exception:
            logger.error(f"❌ Erro ao transformar {f}", exc_info=True)
            continue
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def flatten_children(
    records: Sequence[dict], parent_cols: Sequence[str], child_key: str
) -> list[dict]:
    """Uma linha por filho, com as colunas do pai repetidas (``{**pai, **filho}``)."""
    rows = []
    for rec in records:
        parent = {k: rec.get(k) for k in parent_cols}
        for child in rec.get(child_key) or []:
            rows.append({**parent, **child})
    return rows
