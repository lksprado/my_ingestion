"""Carga de arquivos JSON em tabelas JSONB via COPY.

Cada registro vira uma linha ``(payload JSONB, source_filename TEXT)``; a
tabela de controle registra os arquivos já ingeridos, o que torna a carga
incremental idempotente (arquivo já registrado é pulado).
"""

import io
import json
import logging
from collections.abc import Iterable
from pathlib import Path

from core.db import PostgresClient, validate_raw_schema

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
    ``<schema>.<control_table>`` guarda os arquivos já ingeridos por tabela; cargas
    com ``overwrite=False`` só inserem os que ainda não constam lá.
    """

    def __init__(
        self,
        db: PostgresClient,
        *,
        schema: str,
        control_table: str = "ingestion_control",
        log: logging.Logger | None = None,
    ):
        self.db = db
        self.schema = validate_raw_schema(schema)
        self.control_table = control_table
        self.logger = log or logger

    # ------------------------ DDL ------------------------
    def _ensure_table(self, cur, table: str) -> None:
        cur.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema};
            CREATE TABLE IF NOT EXISTS {self.schema}.{table} (
                payload JSONB NOT NULL,
                source_filename TEXT NOT NULL
            );
            """
        )

    def _ensure_control_table(self, cur) -> None:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.control_table} (
                table_schema TEXT NOT NULL,
                table_name   TEXT NOT NULL,
                filename     TEXT NOT NULL,
                ingested_at  TIMESTAMP NOT NULL DEFAULT now(),
                is_overwrite BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (table_schema, table_name, filename)
            );
            """
        )

    def _truncate(self, cur, table: str) -> None:
        cur.execute(f"TRUNCATE TABLE {self.schema}.{table}")
        cur.execute(
            f"DELETE FROM {self.schema}.{self.control_table} "
            "WHERE table_schema = %s AND table_name = %s",
            (self.schema, table),
        )
        self.logger.info(f"🧹 Tabela truncada: {self.schema}.{table}")

    # ------------------------ controle ------------------------
    def _ingested_filenames(self, cur, table: str) -> set[str]:
        cur.execute(
            f"SELECT filename FROM {self.schema}.{self.control_table} "
            "WHERE table_schema = %s AND table_name = %s",
            (self.schema, table),
        )
        return {row[0] for row in cur.fetchall()}

    def _register(self, cur, table: str, filename: str, overwrite: bool) -> None:
        cur.execute(
            f"INSERT INTO {self.schema}.{self.control_table} "
            "(table_schema, table_name, filename, is_overwrite) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (self.schema, table, filename, overwrite),
        )

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
                self._ensure_control_table(cur)
                if overwrite:
                    self._truncate(cur, table)
                    new_files = files
                else:
                    ingested = self._ingested_filenames(cur, table)
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
                    self._register(cur, table, filepath.name, overwrite)
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
