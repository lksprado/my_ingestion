"""Carga de arquivos JSON em tabelas JSONB via COPY.

Cada registro vira uma linha ``(payload JSONB, source_filename TEXT,
loaded_at_utc)``; a tabela de controle registra os arquivos já ingeridos, o que
torna a carga incremental idempotente (arquivo já registrado é pulado).
``loaded_at_utc`` vem de ``DEFAULT``, como nas tabelas tabulares.
"""

import io
import json
import logging
from collections.abc import Iterable
from pathlib import Path

from core.control import CONTROL_TABLE, IngestionControl
from core.db import (
    LOADED_AT_COLUMN,
    LOADED_AT_DEFAULT,
    PostgresClient,
    ensure_loaded_at,
    validate_raw_schema,
)

logger = logging.getLogger(__name__)


def json_file_to_ndjson_buffer(
    path: Path | str,
    array_key: str | None = None,
    source_filename: str | None = None,
) -> io.StringIO:
    """Serializa um JSON em linhas ``<json>\\t<source_filename>`` para COPY (text).

    - lista → um registro por item;
    - dict com ``array_key`` → um registro por item de ``data[array_key]``;
    - dict sem ``array_key`` → um único registro.

    Levanta ``ValueError`` se o arquivo não for JSON válido ou se ``array_key``
    não apontar para uma lista.
    """
    path = Path(path)
    source_filename = source_filename or path.name

    with path.open("r", encoding="utf-8") as f:
        raw_text = f.read()
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON inválido em {path}: {e}") from e

    if isinstance(data, list):
        iterable = data
    elif isinstance(data, dict) and array_key:
        if not isinstance(data.get(array_key), list):
            raise ValueError(
                f"array_key '{array_key}' ausente ou não é lista em {path}"
            )
        iterable = data[array_key]
    else:
        iterable = [data]

    buffer = io.StringIO()
    for item in iterable:
        json_text = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        # Escape exigido pelo COPY ... FORMAT text
        escaped = (
            json_text.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )
        buffer.write(f"{escaped}\t{source_filename}\n")
    buffer.seek(0)
    return buffer


class JsonbLoader:
    """Carrega JSONs brutos em ``schema.table (payload JSONB, source_filename)``.

    ``schema`` é obrigatório e precisa ser ``raw_<fonte>`` (``validate_raw_schema``).
    ``<schema>.<control_table>`` (``core.control.IngestionControl``) guarda os
    arquivos já ingeridos por tabela; cargas com ``overwrite=False`` só inserem os
    que ainda não constam lá.
    """

    def __init__(
        self,
        db: PostgresClient,
        *,
        schema: str,
        control_table: str = CONTROL_TABLE,
        log: logging.Logger | None = None,
    ):
        self.db = db
        self.schema = validate_raw_schema(schema)
        self.control_table = control_table
        self.logger = log or logger
        self.control = IngestionControl(
            db, schema=self.schema, table=control_table, log=self.logger
        )

    # ------------------------ DDL ------------------------
    def _ensure_table(self, cur, table: str) -> None:
        # loaded_at_utc alinha as tabelas JSONB com as tabulares: é o
        # loaded_at_field do dbt e o que a sincronização prod -> dev usa para
        # achar o delta.
        cur.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema};
            CREATE TABLE IF NOT EXISTS {self.schema}.{table} (
                payload JSONB NOT NULL,
                source_filename TEXT NOT NULL,
                {LOADED_AT_COLUMN} TIMESTAMP NOT NULL
                    DEFAULT ({LOADED_AT_DEFAULT})
            );
            """
        )
        # Mesmo reparo do caminho tabular: cria a coluna quando a tabela é
        # anterior a ela e garante o DEFAULT em UTC.
        ensure_loaded_at(cur, self.schema, table, self.logger)

    def _truncate(self, cur, table: str) -> None:
        cur.execute(f"TRUNCATE TABLE {self.schema}.{table}")
        self.control.clear(cur, table)
        self.logger.info(f"🧹 Tabela truncada: {self.schema}.{table}")

    def _copy(self, cur, table: str, buffer: io.StringIO) -> None:
        cur.copy_expert(
            f"COPY {self.schema}.{table} (payload, source_filename) "
            "FROM STDIN WITH (FORMAT text)",
            buffer,
        )

    # ------------------------ API pública ------------------------
    def load_files(
        self,
        files: Iterable[Path],
        table: str,
        array_key: str | None = None,
        overwrite: bool = False,
    ) -> int:
        """Carrega vários JSONs; devolve quantos arquivos foram inseridos.

        Com ``overwrite=True`` trunca a tabela e recarrega tudo; caso contrário
        insere apenas os arquivos ausentes da tabela de controle.
        """
        files = sorted(Path(f) for f in files)
        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                self._ensure_table(cur, table)
                self.control.ensure(cur)
                if overwrite:
                    self._truncate(cur, table)
                    new_files = files
                else:
                    ingested = self.control.ingested(cur, table)
                    new_files = [f for f in files if f.name not in ingested]

                if not new_files:
                    self.logger.info(
                        f"✅ Nada novo para {self.schema}.{table}: "
                        f"{len(files)} arquivo(s) já ingeridos."
                    )
                    conn.commit()
                    return 0

                target = f"{self.schema}.{table}"
                self.logger.info(f"📤 {len(new_files)} arquivo(s) novo(s) -> {target}")
                for i, filepath in enumerate(new_files, start=1):
                    buffer = json_file_to_ndjson_buffer(filepath, array_key)
                    self._copy(cur, table, buffer)
                    self.control.register(cur, table, [filepath.name], overwrite)
                    if i % 1000 == 0:
                        self.logger.info(f"   ... {i}/{len(new_files)}")
            conn.commit()
            self.logger.info(f"✅ {len(new_files)} arquivo(s) em {self.schema}.{table}")
            return len(new_files)
        except Exception:
            conn.rollback()
            raise
        finally:
            if not self.db.external_connection:
                conn.close()
