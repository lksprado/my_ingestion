"""Extração incremental: por data (high-water mark no banco) e por ID.

Por data (energia/solar, clima/openweather): descobre no Postgres até onde os
dados vão e devolve as datas faltantes, gravando-as num CSV de controle.

Por ID (legislativo): compara três conjuntos — todos os IDs, os já baixados e
os que a API nunca respondeu (CSV "sem dados") — e devolve só os pendentes.
``extract_by_ids`` monta o loop inteiro a partir do YAML: ``base_url`` e
``landing_file`` com o placeholder ``{id}``, ``parameter_file`` com os IDs e, em
``options``, ``no_data_file``, ``parameter_column`` (default ``id``) e
``blacklist_on_error`` (default ``true``).
"""

import csv
import logging
from collections.abc import Callable, Iterable, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from core.config import PipelineConfig
from core.db import PostgresClient
from core.http import HttpClient

logger = logging.getLogger(__name__)


# ------------------------------ por data ------------------------------


def max_date(db: PostgresClient, sql: str) -> date | None:
    """Executa ``sql`` (1ª coluna da 1ª linha = data) e devolve ``date`` ou None."""
    df = db.read_sql(sql)
    if df.empty or df.iloc[0, 0] is None:
        return None
    value = df.iloc[0, 0]
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def missing_dates(
    since: date, cutoff_hour: int = 20, now: datetime | None = None
) -> list[str]:
    """Datas (``YYYY-MM-DD``) de ``since + 1`` até a última data completa.

    A última data é ontem, ou hoje se já passou de ``cutoff_hour`` (fontes com
    resumo diário só fecham o dia à noite). Lista vazia se nada falta.
    """
    now = now or datetime.now()
    last = now.date() if now.hour >= cutoff_hour else (now - timedelta(days=1)).date()
    delta = (last - since).days
    if delta <= 0:
        return []
    return [
        (since + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, delta + 1)
    ]


def missing_dates_from_db(
    db: PostgresClient,
    sqls: Sequence[str],
    control_path: Path | str,
    cutoff_hour: int = 20,
) -> list[str]:
    """Datas faltantes a partir do menor high-water mark de ``sqls``.

    Cada SQL devolve a data máxima de uma tabela; o menor deles é o ponto de
    partida (todas as tabelas precisam alcançá-lo). Levanta ``ValueError`` se
    alguma tabela estiver vazia. Grava o resultado em ``control_path``.
    """
    marks = []
    for sql in sqls:
        mark = max_date(db, sql)
        if mark is None:
            raise ValueError(f"High-water mark vazio para: {sql}")
        marks.append(mark)
    dates = missing_dates(min(marks), cutoff_hour=cutoff_hour)
    write_dates_csv(dates, control_path)
    return dates


def write_dates_csv(dates: list[str], path: Path | str) -> Path:
    """Grava uma data por linha (arquivo vazio se ``dates`` for vazio)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        for d in dates:
            writer.writerow([d])
    logger.info(f"📄 {len(dates)} data(s) em: {path}")
    return path


def read_dates_csv(path: Path | str) -> list[str]:
    """Lê o arquivo de controle gerado por ``write_dates_csv``."""
    path = Path(path)
    if not path.exists():
        return []
    with path.open() as f:
        return [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]


# ------------------------------ por ID ------------------------------


def read_no_data(no_data_path: Path | str | None) -> set[str]:
    """IDs registrados no CSV "sem dados" (vazio se o arquivo não existe)."""
    if no_data_path is None or not Path(no_data_path).exists():
        return set()
    with Path(no_data_path).open(encoding="utf-8") as f:
        rows = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]
    return {r for r in rows if r != "id"}


def pending_ids(
    all_ids: Iterable[str],
    done_ids: Iterable[str],
    no_data_path: Path | str | None = None,
) -> list[str]:
    """``all_ids`` menos os já baixados e os registrados como "sem dados".

    Preserva a ordem de ``all_ids`` e remove duplicatas.
    """
    all_ids = [str(i) for i in all_ids]
    skip = {str(i) for i in done_ids} | read_no_data(no_data_path)
    seen: set[str] = set()
    result = []
    for i in all_ids:
        if i in skip or i in seen:
            continue
        seen.add(i)
        result.append(i)
    logger.info(
        f"{len(result)} pendente(s) de {len(set(all_ids))} "
        f"(ja baixados ou sem dados: {len(skip)})."
    )
    return result


def mark_no_data(no_data_path: Path | str, id_: str) -> None:
    """Registra ``id_`` no CSV "sem dados" (cria com cabeçalho ``id`` na 1ª vez)."""
    path = Path(no_data_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        if write_header:
            f.write("id\n")
        f.write(f"{id_}\n")


def read_ids(path: Path | str, column: str) -> list[str]:
    """IDs únicos de uma coluna de CSV, como str (sem ``.0`` de float)."""
    s = pd.read_csv(path)[column].dropna()
    if pd.api.types.is_numeric_dtype(s):
        s = s.astype("int64")
    return s.astype(str).drop_duplicates().tolist()


def landing_ids(landing_dir: Path, suffix: str) -> set[str]:
    """IDs já baixados: stems dos ``.json`` do landing sem o sufixo."""
    return {f.stem.removesuffix(suffix) for f in landing_dir.glob("*.json")}


def extract_by_ids(
    cfg: PipelineConfig,
    has_data: Callable[[object], bool] = bool,
    http: HttpClient | None = None,
) -> None:
    """Requisita ``cfg.url_base.format(id=...)`` para cada ID pendente.

    Pendente = ``parameter_file`` menos os já no landing e os do ``no_data_file``.
    Resposta em que ``has_data`` é falso vai para o ``no_data_file``; erro/timeout
    também, salvo ``options.blacklist_on_error: false``.
    """
    opts = cfg.options
    no_data = cfg.parameter_dir / opts["no_data_file"]
    # "{id}_votos.json" -> "_votos", para recuperar o id do nome do arquivo
    suffix = Path(cfg.landing_file.replace("{id}", "")).stem
    ids = read_ids(cfg.parameter_filepath, opts.get("parameter_column", "id"))
    todo = pending_ids(ids, landing_ids(cfg.landing_dir, suffix), no_data)
    blacklist_on_error = bool(opts.get("blacklist_on_error", True))
    http = http or HttpClient(logger)

    for id_ in todo:
        data = http.get_json(cfg.url_base.format(id=id_))
        if data is not None and has_data(data):
            http.save_json(data, cfg.landing_dir, cfg.landing_file.format(id=id_))
        elif data is None:
            logger.warning(f"⚠️ Erro/timeout para {id_}.")
            if blacklist_on_error:
                mark_no_data(no_data, id_)
        else:
            logger.warning(f"⚠️ Sem dados para {id_}; registrado em {no_data.name}.")
            mark_no_data(no_data, id_)
