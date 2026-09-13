"""Cliente Postgres único do monorepo.

A conexão vem de ``settings.db_target`` (perfil ``DB__<ENV>__*`` do .env) ou de
um ``DbTarget``/``connection``/``engine`` injetado — assim a ``core`` importa sem
.env (testes, Airflow com ``PostgresClient(connection=hook.get_conn())``).

Toda escrita exige ``schema`` explícito começando com ``raw_`` (``raw_<fonte>``);
``validate_raw_schema`` centraliza a regra.
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from settings import DbTarget

logger = logging.getLogger(__name__)

RAW_SCHEMA_PREFIX = "raw_"
# O schema é interpolado em SQL (JsonbLoader, CREATE SCHEMA): só identificador simples.
_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")


def validate_raw_schema(schema: str | None) -> str:
    """Garante que toda escrita da ingestão vai para um schema ``raw_<fonte>``."""
    if (
        not schema
        or not schema.startswith(RAW_SCHEMA_PREFIX)
        or not _IDENT.match(schema)
    ):
        raise ValueError(
            f"Schema de escrita inválido: {schema!r}. "
            "Use 'raw_<fonte>' (ex.: raw_camara)."
        )
    return schema


def _default_target() -> "DbTarget":
    # Import adiado: a core precisa importar sem .env quando recebe conexão injetada.
    from settings import settings

    return settings.db_target


class PostgresClient:
    def __init__(
        self,
        target: "DbTarget | None" = None,
        *,
        connection=None,  # conexão externa (psycopg2 / PostgresHook.get_conn())
        engine=None,  # engine externa (sqlalchemy)
        log: logging.Logger | None = None,
    ):
        self.external_connection = connection
        self.external_engine = engine
        self.engine = engine
        self.logger = log or logger

        if target is None and not connection and not engine:
            target = _default_target()
        self.target = target

    def connect(self):
        """Conexão psycopg2 (injetada ou nova). Levanta se não conectar.

        Para quem precisa de ``cursor.copy_expert`` ou transação explícita; o
        chamador fecha a conexão (exceto se ela foi injetada).
        """
        if self.external_connection:
            return self.external_connection
        try:
            return psycopg2.connect(
                dbname=self.target.name,
                user=self.target.user,
                password=self.target.password,
                host=self.target.host,
                port=self.target.port,
            )
        except psycopg2.Error as e:
            self.logger.error(f"❌ Erro ao conectar: {e}", exc_info=True)
            raise ConnectionError("Falha ao conectar no Postgres.") from e

    def alchemy(self):
        """Engine SQLAlchemy (injetada ou criada a partir do perfil)."""
        if self.external_engine:
            return self.external_engine
        if self.engine is None:
            self.engine = create_engine(self.target.url)
        return self.engine

    def _ensure_schema(self, schema: str) -> None:
        """``to_sql`` não cria schema; garante ``raw_<fonte>`` antes da carga."""
        # begin(): SQLAlchemy 2.0 não faz autocommit em connect().
        with self.alchemy().begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    def send_df_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        *,
        schema: str,
        how: str = "replace",
        filename: str | None = None,
    ) -> None:
        """Envia um DataFrame para ``schema.table_name`` com colunas de rastreio
        (``arquivo_origem`` se ``filename`` for dado, e ``data_carga``)."""
        schema = validate_raw_schema(schema)
        if filename:
            df["arquivo_origem"] = filename
        df["data_carga"] = datetime.now()

        engine = self.alchemy()
        try:
            self._ensure_schema(schema)
            df.to_sql(
                name=table_name, con=engine, schema=schema, if_exists=how, index=False
            )
            self.logger.info(f"✅ {len(df)} linha(s) em {schema}.{table_name} ({how})")
        except Exception as e:
            self.logger.error(f"❌ Erro ao inserir no banco: {e}", exc_info=True)
            raise

    def load_files_to_table(
        self,
        input_dir: Path | str,
        *,
        schema: str,
        table_name: str | None = None,
        pattern: str = "*.csv",
        how: str = "replace",
        source_column: str = "arquivo_origem",
        sep: str = ";",
    ) -> None:
        """Carrega os arquivos de um diretório (CSV ou JSON) em ``schema.*``.

        - ``table_name`` definido: concatena tudo numa única tabela, com
          ``source_column`` rastreando o arquivo de origem.
        - ``table_name=None``: cada arquivo vira a tabela de mesmo nome (stem).
        """
        schema = validate_raw_schema(schema)
        input_dir = Path(input_dir)
        files = sorted(input_dir.glob(pattern))
        if not files:
            self.logger.warning(f"⚠️ Nenhum arquivo ({pattern}) em {input_dir}")
            return

        def _read(file: Path) -> pd.DataFrame:
            if file.suffix.lower() == ".json":
                return pd.read_json(file, encoding="utf-8")
            return pd.read_csv(file, sep=sep, encoding="utf-8", low_memory=False)

        if table_name:
            dfs = []
            for file in files:
                df = _read(file)
                df[source_column] = file.name
                dfs.append(df)
            self.send_df_to_db(
                pd.concat(dfs, ignore_index=True), table_name, schema=schema, how=how
            )
        else:
            for file in files:
                self.send_df_to_db(
                    _read(file), file.stem, schema=schema, how=how, filename=file.name
                )
        self.logger.info(f"✅ Load concluido: {len(files)} arquivo(s)")

    def read_sql(self, sql: str) -> pd.DataFrame:
        """Resultado de uma query como DataFrame (leituras: staging, intermediate)."""
        con = self.external_connection or self.alchemy()
        return pd.read_sql(sql, con=con)
