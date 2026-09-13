"""Extração incremental por data (high-water mark no banco -> datas faltantes).

Compartilhado por energia/solar e clima/openweather, que descobrem no Postgres
até onde os dados já vão e requisitam só o intervalo faltante.
"""

import csv
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def get_first(db, sql: str):
    """Primeira linha de ``sql``; aceita conexão psycopg2 ou PostgresHook do Airflow."""
    if hasattr(db, "get_first") and callable(db.get_first):
        return db.get_first(sql)
    if hasattr(db, "cursor") and callable(db.cursor):
        with db.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()
    raise TypeError("db deve ser PostgresHook ou conexão psycopg2.")


def get_max_date(db, sql: str) -> date | None:
    """Executa ``sql`` (que deve devolver uma data na 1ª coluna) e retorna ``date``."""
    row = get_first(db, sql)
    if not row or row[0] is None:
        return None
    return datetime.strptime(str(row[0])[:10], "%Y-%m-%d").date()


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
