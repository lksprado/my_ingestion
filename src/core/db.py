"""Cliente Postgres único do monorepo (ex-PostgreSQLManager de demodados).

Absorve também ``load_csvs_to_raw`` (investments) e ``load_data`` (books)
como ``load_files_to_table``. Credenciais vêm de ``settings``
(com fallback para env vars, útil em testes com engine/conexão injetada).
"""

import logging
import os
from datetime import datetime
from logging import NullHandler
from pathlib import Path
from typing import Literal

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())


def _settings_value(field: str) -> str | None:
    """Busca um campo no settings central; cai para os.getenv se indisponível."""
    try:
        from settings import settings

        return getattr(settings, field, None)
    except Exception:
        from dotenv import load_dotenv

        load_dotenv()
        return os.getenv(field.upper())


class PostgresClient:
    def __init__(
        self,
        db_name=None,
        db_user=None,
        db_password=None,
        db_host=None,
        db_port=None,
        connection=None,  # conexão externa (psycopg2)
        engine=None,  # engine externa (sqlalchemy)
        log: logging.Logger | None = None,
    ):
        self.external_connection = connection
        self.external_engine = engine
        self.engine = engine
        self.logger = log or logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.db_name = db_name or _settings_value("db_name")
        self.db_user = db_user or _settings_value("db_user")
        self.db_password = db_password or _settings_value("db_password")
        self.db_host = db_host or _settings_value("db_host")
        self.db_port = db_port or _settings_value("db_port")

        if (
            not self.external_connection
            and not self.external_engine
            and not all([self.db_name, self.db_user, self.db_password, self.db_host])
        ):
            raise ValueError("CREDENCIAIS DO BANCO NAO FORNECIDAS.")

    def _connect(self):
        if self.external_connection:
            self.logger.debug("Usando conexao externa (injetada)")
            return self.external_connection
        try:
            connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
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
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
        return self.engine

    def send_df_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        how: str = "replace",
        filename: str | None = None,
        schema: str = "raw",
    ):
        """Envia um DataFrame para ``schema.table_name``, com colunas de rastreio."""
        if filename:
            df["arquivo_origem"] = filename
        df["data_carga"] = datetime.now()

        engine = self.alchemy()
        try:
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
        filename: str | None = None,
        sep: str = ";",
        chunksize: int = 50_000,
        how: str = "replace",
        schema: str = "raw",
    ):
        """Carrega um CSV grande para ``schema.table_name`` em blocos (streaming).

        Le e insere o CSV em chunks de ``chunksize`` linhas em vez de materializar
        o arquivo inteiro em memoria, evitando OOM com bronzes grandes. O primeiro
        bloco usa ``how`` (replace por padrao) e os demais fazem append.
        """
        engine = self.alchemy()
        total = 0
        first = True
        try:
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
        table_name: str | None = None,
        file_extension: Literal["csv", "json"] = "csv",
        pattern: str | None = None,
        strip_prefix: str = "",
        how: str = "replace",
        schema: str = "raw",
        source_column: str = "arquivo_origem",
        sep: str = ",",
    ) -> None:
        """Carrega arquivos de um diretório para o banco.

        Dois modos:
        - ``table_name`` definido: concatena todos os arquivos numa única tabela,
          com coluna ``source_column`` rastreando o arquivo de origem (ex-load_data).
        - ``table_name=None``: cada arquivo vira uma tabela nomeada pelo stem,
          removendo ``strip_prefix`` (ex-load_csvs_to_raw).
        """
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
                pd.concat(dfs, ignore_index=True), table_name, how=how, schema=schema
            )
        else:
            for file in files:
                self.send_df_to_db(
                    _read(file),
                    file.stem.removeprefix(strip_prefix),
                    how=how,
                    filename=file.name,
                    schema=schema,
                )
        self.logger.info(f"✅ Load concluido: {len(files)} arquivo(s)")

    def execute_query(self, query: str):
        try:
            if self.engine:
                with self.engine.connect() as conn:
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
