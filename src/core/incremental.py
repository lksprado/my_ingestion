"""Extração incremental: por data e por ID, sempre a partir do próprio landing.

Por data (energia/solar, clima/openweather): o landing guarda um JSON por dia
(``{day}`` em ``landing_file``), e as datas faltantes são os dias sem arquivo —
inclusive buracos no meio, que um high-water mark (``MAX(data)``) pularia para
sempre. O resultado vai para um CSV de controle.

Por ID (legislativo): compara três conjuntos — todos os IDs, os já baixados e
os que a API disse não ter (CSV "sem dados") — e devolve só os pendentes.
``extract_by_ids`` monta o loop inteiro a partir do YAML: ``base_url`` e
``landing_file`` com o placeholder ``{id}``, ``parameter_file`` com os IDs e, em
``options``, ``no_data_file``, ``parameter_column`` (default ``id``),
``dias_para_desistir`` (default ``7``) e ``workers`` (default ``1``; > 1 faz as
requisições em threads).
"""

import csv
import logging
import re
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import pandas as pd

from core.config import PipelineConfig
from core.http import HttpClient, ensure_some_success

logger = logging.getLogger(__name__)


# ------------------------------ por data ------------------------------


_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def landing_dates(landing_dir: Path, pattern: str) -> set[date]:
    """Dias (``YYYY-MM-DD`` no nome) dos arquivos de ``landing_dir`` em ``pattern``.

    Arquivo vazio não conta: é resto de escrita interrompida, e contá-lo
    esconderia o buraco para sempre.
    """
    dias = set()
    for f in landing_dir.glob(pattern):
        if (m := _DATE_RE.search(f.name)) and f.stat().st_size > 0:
            dias.add(date.fromisoformat(m.group(0)))
    return dias


def last_complete_date(cutoff_hour: int = 20, now: datetime | None = None) -> date:
    """Ontem, ou hoje depois de ``cutoff_hour`` (o resumo diário fecha à noite)."""
    now = now or datetime.now()
    return now.date() if now.hour >= cutoff_hour else now.date() - timedelta(days=1)


def missing_dates_from_landing(
    cfg: PipelineConfig, now: datetime | None = None
) -> list[str]:
    """Dias completos sem JSON no landing; grava a lista em ``options.control_file``.

    Olha os últimos ``options.lookback_days`` (default 30) dias — o que também
    refaz um dia que falhou no meio — e, sempre, tudo o que vem depois do último
    dia baixado, para uma parada longa não virar buraco. Não volta para antes do
    primeiro dia do landing. Landing vazio: só a janela. ``options.cutoff_hour``
    (default 20) decide se hoje já conta como dia completo.
    """
    opts = cfg.options
    last = last_complete_date(int(opts.get("cutoff_hour", 20)), now)
    have = landing_dates(cfg.landing_dir, cfg.landing_file.format(day="*"))
    start = last - timedelta(days=int(opts.get("lookback_days", 30)) - 1)
    if have:
        start = max(start, min(have))
        start = min(start, max(have) + timedelta(days=1))
    dias = (start + timedelta(days=i) for i in range((last - start).days + 1))
    dates = [d.isoformat() for d in dias if d not in have]
    write_dates_csv(dates, cfg.landing_dir / opts["control_file"])
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


# Respostas que dizem "este ID não existe": vão direto para o "sem dados".
NO_DATA_STATUS = frozenset({404, 410})
Outcome = Literal["ok", "sem_dados", "erro"]


def failures_path(no_data_path: Path) -> Path:
    """CSV ``id,desde`` dos IDs com erro transitório, ao lado do "sem dados"."""
    return no_data_path.with_name(f"{no_data_path.stem}_erros.csv")


def read_failures(path: Path) -> dict[str, str]:
    """``{id: data da primeira falha}``; vazio se o arquivo não existe."""
    if not path.exists():
        return {}
    with path.open(encoding="utf-8", newline="") as f:
        return {row["id"]: row["desde"] for row in csv.DictReader(f)}


def write_failures(path: Path, failures: dict[str, str]) -> None:
    if not failures:
        path.unlink(missing_ok=True)
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "desde"])
        writer.writerows(sorted(failures.items()))


def extract_by_ids(
    cfg: PipelineConfig,
    has_data: Callable[[object], bool] = bool,
    http: HttpClient | None = None,
    today: date | None = None,
) -> None:
    """Requisita ``cfg.url_base.format(id=...)`` para cada ID pendente.

    Pendente = ``parameter_file`` menos os já no landing e os do ``no_data_file``.
    Vão para o ``no_data_file`` só as respostas definitivas: sem dado segundo
    ``has_data`` ou 404/410. Erro transitório (timeout, 5xx, 429...) deixa o ID
    pendente e anota a data da primeira falha no ``<no_data_file>_erros.csv``; o
    ID só é desistido (vai para o "sem dados") depois de falhar por
    ``options.dias_para_desistir`` dias — assim uma API fora do ar por uma
    execução não abre buraco, e um ID que devolve 500 para sempre não é pedido
    para sempre. Se nenhuma requisição dá certo, a etapa falha
    (``ensure_some_success``) — contando só os IDs que não vinham falhando de
    execuções anteriores: um ID quebrado conhecido não pinta a etapa de
    vermelho todo dia até a desistência, mas o retry no mesmo dia de uma API
    fora do ar continua vermelho.

    Com ``options.workers`` > 1 as requisições (e o ``save_json``, um arquivo
    por ID) rodam em threads; os CSVs de controle são sempre escritos pela
    thread principal.
    """
    opts = cfg.options
    no_data = cfg.parameter_dir / opts["no_data_file"]
    erros_path = failures_path(no_data)
    # "{id}_votos.json" -> "_votos", para recuperar o id do nome do arquivo
    suffix = Path(cfg.landing_file.replace("{id}", "")).stem
    ids = read_ids(cfg.parameter_filepath, opts.get("parameter_column", "id"))
    todo = pending_ids(ids, landing_ids(cfg.landing_dir, suffix), no_data)
    dias = int(opts.get("dias_para_desistir", 7))
    today = today or date.today()
    erros = read_failures(erros_path)
    conhecidos = {i for i, desde in erros.items() if desde < today.isoformat()}
    workers = int(opts.get("workers", 1))
    http = http or HttpClient(logger, pool_size=max(10, workers))
    n_falhas = 0

    def fetch(id_: str) -> Outcome:
        data, status = http.get_json_status(cfg.url_base.format(id=id_))
        if data is None:
            return "sem_dados" if status in NO_DATA_STATUS else "erro"
        if not has_data(data):
            return "sem_dados"
        http.save_json(data, cfg.landing_dir, cfg.landing_file.format(id=id_))
        return "ok"

    def record(id_: str, outcome: Outcome) -> None:
        nonlocal n_falhas
        if outcome == "ok":
            erros.pop(id_, None)
            return
        if outcome == "sem_dados":
            logger.warning(f"⚠️ Sem dados para {id_}; registrado em {no_data.name}.")
            erros.pop(id_, None)
            mark_no_data(no_data, id_)
            return
        n_falhas += id_ not in conhecidos
        desde = erros.setdefault(id_, today.isoformat())
        if (today - date.fromisoformat(desde)).days >= dias:
            logger.warning(
                f"⚠️ {id_} falha desde {desde}; desisto e registro em {no_data.name}."
            )
            erros.pop(id_)
            mark_no_data(no_data, id_)

    def unexpected(id_: str, e: Exception) -> None:
        # Falha nossa, não da API: não conta para desistir do ID.
        nonlocal n_falhas
        n_falhas += id_ not in conhecidos
        logger.error(f"❌ Erro em {id_}: {e}", exc_info=True)

    try:
        if workers <= 1:
            for id_ in todo:
                try:
                    record(id_, fetch(id_))
                except Exception as e:
                    unexpected(id_, e)
        else:
            logger.info(f"📥 {len(todo)} ID(s) com {workers} threads...")
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(fetch, id_): id_ for id_ in todo}
                for n, fut in enumerate(as_completed(futures), start=1):
                    id_ = futures[fut]
                    try:
                        record(id_, fut.result())
                    except Exception as e:
                        unexpected(id_, e)
                    if n % 500 == 0:
                        logger.info(f"📊 {n}/{len(todo)} ID(s) concluídos.")
    finally:
        write_failures(erros_path, erros)
    if persistentes := [i for i in todo if i in erros and i in conhecidos]:
        logger.warning(
            f"⚠️ {len(persistentes)} ID(s) seguem falhando de execuções anteriores "
            f"(desistência em {dias} dias): {persistentes[:10]}"
        )
    novos = sum(i not in conhecidos for i in todo)
    ensure_some_success(novos, n_falhas, "ID(s)", log=logger)
