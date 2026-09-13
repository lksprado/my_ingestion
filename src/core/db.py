"""Cliente Postgres único do monorepo (ex-PostgreSQLManager de demodados).

Absorve também ``load_csvs_to_raw`` (investments) e ``load_data`` (books)
como ``load_files_to_table``. A conexão vem de ``settings.db_target`` (perfil
``DB__<ENV>__*`` do .env) ou de um ``DbTarget``/``connection``/``engine``
injetado — assim a ``core`` importa sem .env (testes, Airflow com hook).

Toda escrita exige ``schema`` explícito começando com ``raw_`` (``raw_<fonte>``);
``validate_raw_schema`` centraliza a regra.
"""

import logging
import re
from datetime import datetime
from logging import NullHandler
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from settings import DbTarget

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

RAW_SCHEMA_PREFIX = "raw_"
# O schema é interpolado em SQL (JsonbLoader, CREATE SCHEMA): só identificador simples.
_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")


def validate_raw_schema(schema: str | None) -> str:
    """Garante que toda escrita da ingestão vai para um schema ``raw_<fonte>``.

    Levanta ``ValueError`` para ``None``, prefixo diferente de ``raw_`` ou nome
    que não seja um identificador minúsculo simples.
    """
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
        self.logger = log or logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        if target is None and not connection and not engine:
            target = _default_target()
        self.target = target

    def _connect(self):
        if self.external_connection:
            self.logger.debug("Usando conexao externa (injetada)")
            return self.external_connection
        try:
            connection = psycopg2.connect(
                dbname=self.target.name,
                user=self.target.user,
                password=self.target.password,
                host=self.target.host,
                port=self.target.port,
            )
            self.logger.debug("Conexao ok.")
            return connection
        except psycopg2.Error as e:
            self.logger.error(f"❌ Erro ao conectar: {e}", exc_info=True)
            return None

    def connect(self):
        """Conexão psycopg2 crua (injetada ou nova). Levanta se não conectar.

        Para quem precisa de ``cursor.copy_expert`` ou de transação explícita;
        o chamador fecha a conexão (exceto se ela foi injetada).
        """
        connection = self._connect()
        if connection is None:
            raise ConnectionError("Falha ao conectar no Postgres (ver log acima).")
        return connection

    def alchemy(self):
        if self.external_engine:
            self.logger.debug("Usando engine externa (injetada)")
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
    ):
        """Envia um DataFrame para ``schema.table_name``, com colunas de rastreio."""
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
            self.logger.info(f"✅ Dados inseridos em {schema}.{table_name}")
        except Exception as e:
            self.logger.error(f"❌ Erro ao inserir no banco: {e}", exc_info=True)
            raise

    def send_csv_to_db(
        self,
        csv_path,
        table_name: str,
        *,
        schema: str,
        filename: str | None = None,
        sep: str = ";",
        chunksize: int = 50_000,
        how: str = "replace",
    ):
        """Carrega um CSV grande para ``schema.table_name`` em blocos (streaming).

        Le e insere o CSV em chunks de ``chunksize`` linhas em vez de materializar
        o arquivo inteiro em memoria, evitando OOM com bronzes grandes. O primeiro
        bloco usa ``how`` (replace por padrao) e os demais fazem append.
        """
        schema = validate_raw_schema(schema)
        engine = self.alchemy()
        total = 0
        first = True
        try:
            self._ensure_schema(schema)
            for chunk in pd.read_csv(csv_path, sep=sep, chunksize=chunksize):
                if filename:
                    chunk["arquivo_origem"] = filename
                chunk["data_carga"] = datetime.now()
                # Mantem o INSERT abaixo do limite de 65535 parametros do Postgres.
                rows_per_insert = max(1, 60_000 // max(1, chunk.shape[1]))
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists=how if first else "append",
                    index=False,
                    method="multi",
                    chunksize=rows_per_insert,
                )
                total += len(chunk)
                first = False

            if first:
                # CSV sem linhas (so cabecalho): cria a tabela vazia mesmo assim.
                header = pd.read_csv(csv_path, sep=sep, nrows=0)
                if filename:
                    header["arquivo_origem"] = pd.Series(dtype="object")
                header["data_carga"] = pd.Series(dtype="datetime64[ns]")
                header.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists=how,
                    index=False,
                )

            self.logger.info(f"✅ {total} linhas inseridas em {schema}.{table_name}")
        except Exception as e:
            self.logger.error(f"❌ Erro ao inserir CSV no banco: {e}", exc_info=True)
            raise

    def load_files_to_table(
        self,
        input_dir: Path | str,
        *,
        schema: str,
        table_name: str | None = None,
        file_extension: Literal["csv", "json"] = "csv",
        pattern: str | None = None,
        strip_prefix: str = "",
        how: str = "replace",
        source_column: str = "arquivo_origem",
        sep: str = ",",
    ) -> None:
        """Carrega arquivos de um diretório para ``schema.*``.

        Dois modos:
        - ``table_name`` definido: concatena todos os arquivos numa única tabela,
          com coluna ``source_column`` rastreando o arquivo de origem (ex-load_data).
        - ``table_name=None``: cada arquivo vira uma tabela nomeada pelo stem,
          removendo ``strip_prefix`` (ex-load_csvs_to_raw).
        """
        schema = validate_raw_schema(schema)
        input_dir = Path(input_dir)
        pattern = pattern or f"*.{file_extension}"
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
                    _read(file),
                    file.stem.removeprefix(strip_prefix),
                    schema=schema,
                    how=how,
                    filename=file.name,
                )
        self.logger.info(f"✅ Load concluido: {len(files)} arquivo(s)")

    def execute_query(self, query: str):
        try:
            if self.engine:
                # begin(): garante commit (SQLAlchemy 2.0 não faz autocommit).
                with self.engine.begin() as conn:
                    conn.execute(text(query))
                    self.logger.info("✅ Query executada com sucesso")
            else:
                connection = self._connect()
                cursor = connection.cursor()
                cursor.execute(query)
                cursor.close()
                connection.commit()
                connection.close()
                self.logger.info("✅ Query executada com sucesso")
        except Exception as e:
            self.logger.error(f"❌ Erro ao executar query: {e}", exc_info=True)
            raise

    def fetchone(self, query: str):
        try:
            if self.engine:
                with self.engine.connect() as conn:
                    return conn.execute(text(query)).fetchone()
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            return result
        except Exception as e:
            self.logger.error(f"❌ Erro no fetchone: {e}", exc_info=True)
            raise

    def fetchall(self, query: str):
        """Retorna o resultado completo de uma query como DataFrame."""
        return pd.read_sql(query, con=self.alchemy())
