"""Cliente Postgres único do monorepo.

A conexão vem de ``settings.db_target`` (perfil ``DB__<ENV>__*`` do .env) ou de
um ``DbTarget``/``connection``/``engine`` injetado — assim a ``core`` importa sem
.env (testes, Airflow com ``PostgresClient(connection=hook.get_conn())``).

Toda escrita exige ``schema`` explícito começando com ``raw_`` (``raw_<fonte>``);
``validate_raw_schema`` centraliza a regra.

**A carga é sempre ``COPY``, nunca ``DROP``.** Um full refresh é
``CREATE TABLE IF NOT EXISTS`` + ``TRUNCATE`` + ``COPY`` numa única transação: a
tabela nunca é recriada, então as views do dbt sobre a raw sobrevivem, o OID é
estável (o que torna possível sincronizar prod → dev) e uma falha no meio faz
rollback preservando os dados anteriores.

Na raw os dados são sempre texto: toda coluna é ``TEXT`` (a tipagem é do dbt) —
exceto colunas cujos valores são objetos JSON (dict/list), gravadas como
``JSONB``. As colunas de rastreio ``arquivo_origem`` e ``loaded_at_utc`` nunca
viajam no stream do COPY: vêm de ``DEFAULT`` no catálogo, então ``loaded_at_utc``
é o ``now() AT TIME ZONE 'utc'`` da transação — um valor só para a carga
inteira, sempre em UTC, qualquer que seja o fuso do servidor.

Convenção de NULL no COPY (``FORMAT csv``, marcador default ``''``):

- **caminho do bronze** (``copy_csv``): campo vazio não aspado é NULL — e só ele,
  de modo que ``NA``, ``null`` e ``nan`` seguem texto e ``007`` segue ``007``;
- **caminho em memória** (``send_df_to_db``): o buffer aspa tudo que não é
  ``None``. Assim ``''`` sai como ``""`` (string vazia) e ``None`` sai como campo
  vazio (NULL), preservando a distinção entre os dois. De quebra, como todo valor
  não nulo sai aspado, ``;``, ``"``, quebra de linha e a linha ``\\.`` ficam imunes.
"""

import csv
import io
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from settings import DbTarget

logger = logging.getLogger(__name__)

RAW_SCHEMA_PREFIX = "raw_"
# O schema é interpolado em SQL (JsonbLoader, CREATE SCHEMA): só identificador simples.
_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")

WriteMode = Literal["truncate", "append", "merge"]
WRITE_MODES: tuple[str, ...] = ("truncate", "append", "merge")

# Metadados da carga: não vêm do dado, vêm de DEFAULT no catálogo.
LOADED_AT_COLUMN = "loaded_at_utc"
TRACKING_COLUMNS: tuple[str, ...] = ("arquivo_origem", LOADED_AT_COLUMN)

# `now()` é timestamptz: gravado numa coluna sem fuso, viraria a hora local do
# servidor. O `AT TIME ZONE 'utc'` é o que faz o nome da coluna ser verdade em
# qualquer banco, sem depender do TimeZone da sessão.
LOADED_AT_DEFAULT = "now() AT TIME ZONE 'utc'"
# Como o pg_get_expr normaliza a expressão acima.
_LOADED_AT_DEFAULT_FORM = "(now() AT TIME ZONE 'utc'::text)"

# Sem isso um TRUNCATE entra na fila na frente dos SELECTs do dbt e segura o
# banco pelo tempo inteiro do COPY. Melhor falhar rápido e reexecutar.
LOCK_TIMEOUT = "30s"


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


def validate_write_mode(write: str | None) -> str:
    """Garante um modo de escrita conhecido (``truncate``/``append``/``merge``)."""
    if write not in WRITE_MODES:
        raise ValueError(f"write={write!r} inválido; use um de {WRITE_MODES}.")
    return write


# Leitura de CSV/JSON para a raw: tudo como texto, e só a célula vazia vira NULL
# (sem "NA", "null", "nan"... virando nulo nem "007" virando 7).
READ_CSV_AS_TEXT = {"dtype": str, "keep_default_na": False, "na_values": [""]}


def _is_json_obj(value) -> bool:
    return isinstance(value, dict | list)


def _cell_to_text(value) -> str | None:
    if _is_json_obj(value):
        return json.dumps(value, ensure_ascii=False)
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    return value if isinstance(value, str) else str(value)


def to_raw_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """``(df, tipos)`` para a carga: toda coluna ``TEXT``, exceto JSON -> ``JSONB``.

    Coluna em que todo valor não nulo é dict/list vira ``JSONB`` (nulos -> NULL) e
    mantém os objetos, serializados na hora de escrever o buffer. Nas demais, cada
    valor vira ``str`` (dict/list soltos em ``json.dumps``) e nulos viram NULL.
    """
    out = pd.DataFrame(index=df.index)
    tipos: dict[str, str] = {}
    for col in df.columns:
        values = df[col].astype(object)
        if pd.api.types.infer_dtype(values, skipna=True) in ("string", "empty"):
            # caminho rápido: CSV lido com READ_CSV_AS_TEXT já é str ou NaN
            out[col] = values.where(values.notna(), None)
            tipos[col] = "TEXT"
            continue
        present = values[values.map(lambda v: _is_json_obj(v) or not pd.isna(v))]
        if len(present) and present.map(_is_json_obj).all():
            cells = [v if _is_json_obj(v) else None for v in values]
            tipos[col] = "JSONB"
        else:
            cells = [_cell_to_text(v) for v in values]
            tipos[col] = "TEXT"
        # dtype=object: None fica None (Series.map devolveria NaN)
        out[col] = pd.Series(cells, index=df.index, dtype=object)
    return out, tipos


# ------------------------ serialização para COPY ------------------------


def _csv_field(value: str | None) -> str:
    """Campo para ``COPY ... FORMAT csv``: ``None`` vazio (NULL), resto aspado.

    Aspar todo valor não nulo é o que preserva a diferença entre NULL e string
    vazia — e, de quebra, torna ``;``, ``"``, quebra de linha e a linha ``\\.``
    inofensivos, sem depender de heurística de quoting.
    """
    if value is None:
        return ""
    return '"' + value.replace('"', '""') + '"'


def df_to_csv_buffer(
    df: pd.DataFrame, tipos: dict[str, str], sep: str = ";"
) -> io.StringIO:
    """Serializa um DataFrame já normalizado para ``COPY ... FORMAT csv``.

    Colunas ``JSONB`` saem como texto JSON compacto. Não usa ``csv.writer``: ele
    aspa o campo único vazio (para a linha não ficar em branco), e isso faria o
    NULL de uma tabela de **uma** coluna virar string vazia. Linha em branco é
    justamente como o COPY representa esse NULL.
    """
    buffer = io.StringIO()
    jsonb = [tipos.get(col) == "JSONB" for col in df.columns]
    for row in df.itertuples(index=False, name=None):
        buffer.write(
            sep.join(
                _csv_field(
                    json.dumps(v, ensure_ascii=False, separators=(",", ":"))
                    if is_json and v is not None
                    else v
                )
                for v, is_json in zip(row, jsonb, strict=True)
            )
        )
        buffer.write("\n")
    buffer.seek(0)
    return buffer


def read_csv_header(path: Path | str, sep: str = ";") -> list[str]:
    """Nomes de coluna da primeira linha de um CSV, na ordem do arquivo."""
    # utf-8-sig: se houver BOM, o decoder o consome e ele não vira parte do nome.
    with open(path, encoding="utf-8-sig", newline="") as f:
        header = next(csv.reader(f, delimiter=sep), None)
    if not header:
        raise ValueError(f"CSV sem cabeçalho: {path}")
    return header


# ------------------------ DDL e reconciliação ------------------------


@dataclass(frozen=True)
class ColumnPlan:
    """Reconciliação entre as colunas do dado e as que a tabela já tem."""

    copy: list[str]  # colunas do COPY, na ordem do dado
    add: list[str]  # estão no dado e faltam na tabela -> ADD COLUMN
    missing: list[str]  # estão na tabela e faltam no dado -> chegam NULL


def plan_columns(data_columns: list[str], table_columns: list[str]) -> ColumnPlan:
    """Plano puro de drift — sem tocar no banco, para poder testar sem Postgres."""
    existing = set(table_columns)
    known = set(data_columns) | set(TRACKING_COLUMNS)
    return ColumnPlan(
        copy=list(data_columns),
        add=[c for c in data_columns if c not in existing],
        missing=[c for c in table_columns if c not in known],
    )


def _table_columns(cur, schema: str, table: str) -> dict[str, str]:
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
    )
    return {row[0]: row[1] for row in cur.fetchall()}


_COLUMN_DEFAULT_SQL = """
    SELECT pg_get_expr(d.adbin, d.adrelid)
    FROM pg_attribute a
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE a.attrelid = %s::regclass AND a.attname = %s AND a.attnum > 0
"""


def ensure_column_default(
    cur, schema: str, table: str, column: str, value: str | None
) -> None:
    """Define o DEFAULT da coluna de rastreio; no-op quando já é o mesmo.

    ``value=None`` significa o carimbo de carga (``LOADED_AT_DEFAULT``). A
    condicional importa: o ``ALTER`` pega ``ACCESS EXCLUSIVE`` até o commit, o
    que em ``write="append"`` bloquearia os leitores durante o COPY inteiro.
    Como ``filename`` é constante entre execuções, da segunda carga em diante
    nenhum ALTER é emitido.
    """
    cur.execute(_COLUMN_DEFAULT_SQL, (f"{schema}.{table}", column))
    atual = cur.fetchone()
    if value is None:
        if atual and atual[0] == _LOADED_AT_DEFAULT_FORM:
            return
    else:
        esperado = "'{}'::text".format(value.replace("'", "''"))
        if atual and atual[0] == esperado:
            return
    cur.execute(
        sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(column),
            sql.SQL(LOADED_AT_DEFAULT) if value is None else sql.Literal(value),
        )
    )


# indisunique cobre também a primary key.
_UNIQUE_INDEX_SQL = """
    SELECT i.indexrelid::regclass::text
    FROM pg_index i
    WHERE i.indrelid = %s::regclass AND i.indisunique
"""

_INDEX_COLUMNS_SQL = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indexrelid = %s::regclass
"""


def _ensure_unique_index(
    cur, schema: str, table: str, colunas: list[str], log: logging.Logger
) -> None:
    """Garante índice único sobre ``colunas`` (o que o ``ON CONFLICT`` exige).

    Procura por **conjunto de colunas**, não por nome: as tabelas de openweather e
    solar já têm índices feitos à mão (``openweather_date_pk`` etc.) e criar um
    segundo idêntico seria desperdício.
    """
    cur.execute(_UNIQUE_INDEX_SQL, (f"{schema}.{table}",))
    for (indice,) in cur.fetchall():
        cur.execute(_INDEX_COLUMNS_SQL, (indice,))
        if {r[0] for r in cur.fetchall()} == set(colunas):
            return
    nome = f"{table}_merge_key"
    cur.execute(
        sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
            sql.Identifier(nome),
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in colunas),
        )
    )
    log.info(f"🔑 Índice único {nome} em {schema}.{table} ({', '.join(colunas)})")


def ensure_loaded_at(
    cur, schema: str, table: str, log: logging.Logger | None = None
) -> None:
    """Garante ``loaded_at_utc`` com o DEFAULT em UTC — tabular e JSONB.

    Reparo idempotente e de catálogo (nenhuma linha é reescrita): tabela que não
    tem a coluna ganha o ``ADD COLUMN`` — senão ela entraria NULL e quebraria o
    ``loaded_at_field`` do dbt — e o DEFAULT é acertado para
    ``now() AT TIME ZONE 'utc'``. As linhas que já estavam lá ficam com o
    instante do ``ADD COLUMN``, não com o da ingestão que as trouxe.
    """
    log = log or logger
    if LOADED_AT_COLUMN not in _table_columns(cur, schema, table):
        cur.execute(
            sql.SQL(
                f"ALTER TABLE {{}}.{{}} ADD COLUMN {LOADED_AT_COLUMN} "
                f"TIMESTAMP DEFAULT ({LOADED_AT_DEFAULT})"
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )
        log.warning(f"➕ {schema}.{table}.{LOADED_AT_COLUMN} criada")
    ensure_column_default(cur, schema, table, LOADED_AT_COLUMN, None)


def ensure_raw_table(
    cur,
    schema: str,
    table: str,
    tipos: dict[str, str],
    *,
    filename: str | None = None,
    merge_key: list[str] | None = None,
    log: logging.Logger | None = None,
) -> ColumnPlan:
    """Garante schema, tabela e colunas de rastreio; devolve o plano de colunas.

    Idempotente e sem DROP: cria o que falta, acrescenta coluna nova do dado e
    avisa (WARNING) sobre coluna que sumiu ou trocou de tipo. Rename e troca de
    tipo exigem recreate manual (documentado no ``core/README.md``).
    """
    log = log or logger
    schema = validate_raw_schema(schema)
    cur.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
    )

    colunas = [
        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in tipos.items()
    ]
    if filename is not None:
        colunas.append(sql.SQL("arquivo_origem TEXT"))
    colunas.append(
        sql.SQL(f"{LOADED_AT_COLUMN} TIMESTAMP NOT NULL DEFAULT ({LOADED_AT_DEFAULT})")
    )
    cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema), sql.Identifier(table), sql.SQL(", ").join(colunas)
        )
    )

    ensure_loaded_at(cur, schema, table, log)
    existentes = _table_columns(cur, schema, table)
    plano = plan_columns(list(tipos), list(existentes))

    for col in plano.add:
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(col),
                sql.SQL(tipos[col]),
            )
        )
        log.warning(f"➕ Coluna nova no dado, adicionada em {schema}.{table}: {col}")
    if plano.missing:
        log.warning(
            f"⚠️ Colunas ausentes no dado, ficarão NULL em {schema}.{table}: "
            f"{plano.missing}"
        )
    for col, tipo in tipos.items():
        atual = existentes.get(col)
        if atual and atual.lower() != tipo.lower():
            log.warning(
                f"⚠️ {schema}.{table}.{col} é {atual} e o dado pede {tipo.lower()}; "
                "o COPY vai gravar no tipo atual. Recreate manual para corrigir."
            )

    if filename is not None:
        if "arquivo_origem" not in existentes:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN arquivo_origem TEXT").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
        ensure_column_default(cur, schema, table, "arquivo_origem", filename)

    if merge_key:
        _ensure_unique_index(cur, schema, table, merge_key, log)

    return plano


def _copy_sql(destino, columns: list[str], sep: str, header: bool):
    """``COPY`` para ``destino`` (tabela real ou a temporária do merge)."""
    return sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER {}, DELIMITER {})"
    ).format(
        destino,
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        sql.SQL("true" if header else "false"),
        sql.Literal(sep),
    )


def _qualified(schema: str, table: str):
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))


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

        É o caminho de toda escrita (``COPY``); o chamador fecha a conexão,
        exceto se ela foi injetada.
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
                # O COPY não pode depender do encoding default do servidor.
                client_encoding="UTF8",
            )
        except psycopg2.Error as e:
            self.logger.error(f"❌ Erro ao conectar: {e}", exc_info=True)
            raise ConnectionError("Falha ao conectar no Postgres.") from e

    def alchemy(self):
        """Engine SQLAlchemy (injetada ou criada a partir do perfil). Só leitura."""
        if self.external_engine:
            return self.external_engine
        if self.engine is None:
            self.engine = create_engine(self.target.url)
        return self.engine

    # ------------------------ carga ------------------------

    def _merge(
        self,
        cur,
        schema: str,
        table: str,
        temp: str,
        colunas: list[str],
        merge_key: list[str],
    ) -> int:
        """``INSERT ... ON CONFLICT DO UPDATE`` da temporária para a tabela.

        ``DISTINCT ON`` não é opcional: o ``ON CONFLICT`` erra com "cannot affect
        row a second time" se o lote trouxer a mesma chave duas vezes.
        """
        atualizar = [c for c in colunas if c not in merge_key]
        lista = sql.SQL(", ").join(sql.Identifier(c) for c in colunas)
        chave = sql.SQL(", ").join(sql.Identifier(c) for c in merge_key)
        cur.execute(
            sql.SQL(
                "INSERT INTO {} ({}) SELECT DISTINCT ON ({}) {} FROM {} ORDER BY {} "
                "ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                _qualified(schema, table),
                lista,
                chave,
                lista,
                sql.Identifier(temp),
                chave,
                chave,
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(c), sql.Identifier(c)
                    )
                    for c in atualizar
                ),
            )
        )
        return cur.rowcount

    def _load(
        self,
        schema: str,
        table: str,
        *,
        tipos: dict[str, str],
        write: str,
        filename: str | None,
        merge_key: list[str] | None = None,
        after_copy=None,
        copy,
    ) -> None:
        """DDL + TRUNCATE/COPY (ou COPY + merge) numa transação só.

        ``after_copy(cur)`` roda antes do commit, para quem precisa gravar algo
        junto com a carga — hoje, o registro do manifesto na tabela de controle.

        DDL no Postgres é transacional, então nada fica visível antes do commit:
        falha no meio devolve a tabela ao estado anterior, nunca vazia ou parcial.
        """
        if write == "merge":
            if not merge_key:
                raise ValueError("write='merge' exige merge_key.")
            faltam = [c for c in merge_key if c not in tipos]
            if faltam:
                raise ValueError(
                    f"merge_key {faltam} não está(ão) nas colunas do dado "
                    f"({list(tipos)})."
                )

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
                plano = ensure_raw_table(
                    cur,
                    schema,
                    table,
                    tipos,
                    filename=filename,
                    merge_key=merge_key if write == "merge" else None,
                    log=self.logger,
                )
                if write == "merge":
                    # LIKE ... INCLUDING DEFAULTS: a temporária já carimba
                    # arquivo_origem e loaded_at_utc com os mesmos DEFAULTs do alvo.
                    temp = f"stg_{table}"[:63]
                    cur.execute(
                        sql.SQL(
                            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) "
                            "ON COMMIT DROP"
                        ).format(sql.Identifier(temp), _qualified(schema, table))
                    )
                    copy(cur, sql.Identifier(temp), plano.copy)
                    copiadas = cur.rowcount
                    colunas = plano.copy + list(
                        c
                        for c in TRACKING_COLUMNS
                        if c == LOADED_AT_COLUMN or filename is not None
                    )
                    linhas = self._merge(cur, schema, table, temp, colunas, merge_key)
                    if linhas < copiadas:
                        self.logger.warning(
                            f"⚠️ {copiadas - linhas} linha(s) com merge_key repetida "
                            f"no lote; só a primeira de cada chave entrou."
                        )
                else:
                    if write == "truncate":
                        cur.execute(
                            sql.SQL("TRUNCATE TABLE {}").format(
                                _qualified(schema, table)
                            )
                        )
                    copy(cur, _qualified(schema, table), plano.copy)
                    linhas = cur.rowcount
                if after_copy is not None:
                    after_copy(cur)
            conn.commit()
            self.logger.info(f"✅ {linhas} linha(s) em {schema}.{table} ({write})")
        except Exception as e:
            conn.rollback()
            self.logger.error(
                f"❌ Erro ao carregar {schema}.{table}: {e}", exc_info=True
            )
            raise
        finally:
            if not self.external_connection:
                conn.close()

    def copy_csv(
        self,
        path: Path | str,
        table_name: str,
        *,
        schema: str,
        sep: str = ";",
        write: str = "truncate",
        filename: str | None = None,
        merge_key: list[str] | None = None,
        after_copy=None,
    ) -> None:
        """Carrega um CSV em ``schema.table_name`` por ``COPY``, sem pandas.

        As colunas (e a ordem) vêm do cabeçalho do arquivo; todas entram ``TEXT``.
        """
        schema = validate_raw_schema(schema)
        validate_write_mode(write)
        path = Path(path)
        tipos = {col: "TEXT" for col in read_csv_header(path, sep)}
        self.logger.info(f"📤 {path} -> {schema}.{table_name} ({write})")
        with open(path, encoding="utf-8-sig", newline="") as fh:
            self._load(
                schema,
                table_name,
                tipos=tipos,
                write=write,
                filename=filename,
                merge_key=merge_key,
                after_copy=after_copy,
                copy=lambda cur, destino, cols: cur.copy_expert(
                    _copy_sql(destino, cols, sep, header=True).as_string(cur), fh
                ),
            )

    def send_df_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        *,
        schema: str,
        write: str = "truncate",
        filename: str | None = None,
        merge_key: list[str] | None = None,
    ) -> None:
        """Envia um DataFrame para ``schema.table_name`` por ``COPY``.

        Dados como ``TEXT`` (JSON como ``JSONB``), ver ``to_raw_frame``.
        ``arquivo_origem`` (se ``filename``) e ``loaded_at_utc`` vêm de DEFAULT.
        """
        schema = validate_raw_schema(schema)
        validate_write_mode(write)
        df, tipos = to_raw_frame(df)
        buffer = df_to_csv_buffer(df, tipos)
        self._load(
            schema,
            table_name,
            tipos=tipos,
            write=write,
            filename=filename,
            merge_key=merge_key,
            copy=lambda cur, destino, cols: cur.copy_expert(
                _copy_sql(destino, cols, ";", header=False).as_string(cur), buffer
            ),
        )

    def load_files_to_table(
        self,
        input_dir: Path | str,
        *,
        schema: str,
        table_name: str | None = None,
        pattern: str = "*.csv",
        write: str = "truncate",
        source_column: str = "arquivo_origem",
        sep: str = ";",
        merge_key: list[str] | None = None,
    ) -> None:
        """Carrega os arquivos de um diretório (CSV ou JSON) em ``schema.*``.

        - ``table_name`` definido: concatena tudo numa única tabela, com
          ``source_column`` rastreando o arquivo de origem linha a linha.
        - ``table_name=None``: cada arquivo vira a tabela de mesmo nome (stem);
          os CSVs vão direto para o ``COPY``, sem passar por pandas.
        """
        schema = validate_raw_schema(schema)
        validate_write_mode(write)
        input_dir = Path(input_dir)
        files = sorted(input_dir.glob(pattern))
        if not files:
            self.logger.warning(f"⚠️ Nenhum arquivo ({pattern}) em {input_dir}")
            return

        def _read_json(file: Path) -> pd.DataFrame:
            return pd.read_json(
                file, encoding="utf-8", dtype=False, convert_dates=False
            )

        if table_name:
            dfs = []
            for file in files:
                if file.suffix.lower() == ".json":
                    df = _read_json(file)
                else:
                    df = pd.read_csv(
                        file, sep=sep, encoding="utf-8", **READ_CSV_AS_TEXT
                    )
                df[source_column] = file.name
                dfs.append(df)
            self.send_df_to_db(
                pd.concat(dfs, ignore_index=True),
                table_name,
                schema=schema,
                write=write,
            )
        else:
            for file in files:
                if file.suffix.lower() == ".json":
                    self.send_df_to_db(
                        _read_json(file),
                        file.stem,
                        schema=schema,
                        write=write,
                        filename=file.name,
                        merge_key=merge_key,
                    )
                else:
                    self.copy_csv(
                        file,
                        file.stem,
                        schema=schema,
                        sep=sep,
                        write=write,
                        filename=file.name,
                        merge_key=merge_key,
                    )
        self.logger.info(f"✅ Load concluido: {len(files)} arquivo(s)")

    def read_sql(self, sql_text: str) -> pd.DataFrame:
        """Resultado de uma query como DataFrame (leituras em objetos do dbt)."""
        if self.external_connection:
            return pd.read_sql(sql_text, con=self.external_connection)
        # text(): '%' literal (LIKE) não vira placeholder do driver.
        return pd.read_sql(text(sql_text), con=self.alchemy())
