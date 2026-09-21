"""Carga por COPY contra um Postgres real (``ingestion_sandbox`` em dev).

Estes são os testes que protegem a raw: fidelidade do CSV (vazio vs NULL, texto
que parece nulo, separador e aspas embutidos), atomicidade do full refresh,
reconciliação de colunas e a compatibilidade com as tabelas legadas criadas pelo
``to_sql`` antigo — nada disso dá para verificar com dublê.
"""

import pandas as pd
import psycopg2
import pytest

from core.db import PostgresClient

pytestmark = pytest.mark.integration

SCHEMA = "raw_pytest_copy"


@pytest.fixture
def pg():
    """Cliente apontando para o banco do ambiente, com um schema descartável."""
    client = PostgresClient()
    conn = client.connect()
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {SCHEMA}")
    conn.commit()
    conn.close()
    yield client
    conn = client.connect()
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.commit()
    conn.close()


def query(pg, sql):
    conn = pg.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def bronze(tmp_path, conteudo, nome="f.csv"):
    path = tmp_path / nome
    path.write_text(conteudo, encoding="utf-8")
    return path


# ------------------------ fidelidade do CSV ------------------------


def test_copy_csv_preserva_a_semantica_de_texto_da_raw(pg, tmp_path):
    path = bronze(
        tmp_path,
        "id;sigla;valor;obs\n"
        '007;NA;1.50;"tem;ponto"\n'
        ';null;;"tem ""aspas"""\n'
        "x;nan;3;acentuação\n",
    )
    pg.copy_csv(path, "texto", schema=SCHEMA, filename=path.name)

    linhas = query(
        pg, f"SELECT id, sigla, valor, obs FROM {SCHEMA}.texto ORDER BY 1 NULLS FIRST"
    )
    assert linhas == [
        (None, "null", None, 'tem "aspas"'),  # só a célula vazia virou NULL
        ("007", "NA", "1.50", "tem;ponto"),  # zero à esquerda preservado
        ("x", "nan", "3", "acentuação"),
    ]
    tipos = query(
        pg,
        "SELECT data_type FROM information_schema.columns "
        f"WHERE table_schema = '{SCHEMA}' AND table_name = 'texto' "
        "AND column_name NOT IN ('loaded_at_utc')",
    )
    assert {t[0] for t in tipos} == {"text"}


def test_copy_csv_com_so_cabecalho_cria_tabela_vazia(pg, tmp_path):
    path = bronze(tmp_path, "a;b\n")
    pg.copy_csv(path, "vazia", schema=SCHEMA, filename=path.name)
    assert query(pg, f"SELECT count(*) FROM {SCHEMA}.vazia") == [(0,)]


def test_send_df_to_db_grava_json_como_jsonb(pg):
    df = pd.DataFrame({"n": ["1"], "j": [{"k": "v", "l": [1, 2]}]})
    pg.send_df_to_db(df, "comjson", schema=SCHEMA)

    assert query(pg, f"SELECT j ->> 'k', j -> 'l' ->> 1 FROM {SCHEMA}.comjson") == [
        ("v", "2")
    ]


def test_send_df_to_db_distingue_nulo_de_string_vazia(pg):
    df = pd.DataFrame({"a": [None, "", "x"]}, dtype=object)
    pg.send_df_to_db(df, "nulos", schema=SCHEMA)

    assert query(
        pg, f"SELECT a IS NULL, a FROM {SCHEMA}.nulos ORDER BY a NULLS FIRST"
    ) == [
        (True, None),
        (False, ""),
        (False, "x"),
    ]


# ------------------------ colunas de rastreio ------------------------


def test_loaded_at_utc_e_um_so_para_a_carga_inteira(pg, tmp_path):
    path = bronze(tmp_path, "a\n" + "\n".join(str(i) for i in range(500)) + "\n")
    pg.copy_csv(path, "rastreio", schema=SCHEMA, filename=path.name)

    assert query(
        pg,
        "SELECT count(DISTINCT loaded_at_utc),"
        " count(*) FILTER (WHERE loaded_at_utc IS NULL),"
        " count(DISTINCT arquivo_origem), min(arquivo_origem)"
        f" FROM {SCHEMA}.rastreio",
    ) == [(1, 0, 1, "f.csv")]


def test_sem_filename_nao_cria_coluna_de_arquivo(pg):
    pg.send_df_to_db(pd.DataFrame({"a": ["1"]}), "semarquivo", schema=SCHEMA)
    colunas = query(
        pg,
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{SCHEMA}' AND table_name = 'semarquivo'",
    )
    assert {c[0] for c in colunas} == {"a", "loaded_at_utc"}


# ------------------------ modos de escrita ------------------------


def test_truncate_substitui_sem_recriar_a_tabela(pg, tmp_path):
    path = bronze(tmp_path, "a\n1\n2\n")
    pg.copy_csv(path, "refresh", schema=SCHEMA, filename=path.name)
    oid = query(pg, f"SELECT '{SCHEMA}.refresh'::regclass::oid")

    bronze(tmp_path, "a\n3\n")
    pg.copy_csv(path, "refresh", schema=SCHEMA, filename=path.name)

    assert query(pg, f"SELECT a FROM {SCHEMA}.refresh") == [("3",)]
    # mesmo OID: a tabela não foi recriada, é isso que preserva views e grants
    assert query(pg, f"SELECT '{SCHEMA}.refresh'::regclass::oid") == oid


def test_append_acumula(pg, tmp_path):
    path = bronze(tmp_path, "a\n1\n")
    pg.copy_csv(path, "acumula", schema=SCHEMA, filename=path.name)
    pg.copy_csv(path, "acumula", schema=SCHEMA, write="append", filename=path.name)
    assert query(pg, f"SELECT count(*) FROM {SCHEMA}.acumula") == [(2,)]


def test_falha_no_meio_do_copy_preserva_os_dados_anteriores(pg, tmp_path):
    path = bronze(tmp_path, "a;b\n1;2\n")
    pg.copy_csv(path, "atomica", schema=SCHEMA, filename=path.name)

    # linha com mais campos que o cabeçalho: o COPY falha no meio
    ruim = bronze(tmp_path, "a;b\n3;4\n5;6;7\n", nome="ruim.csv")
    with pytest.raises(psycopg2.Error):
        pg.copy_csv(ruim, "atomica", schema=SCHEMA, filename=ruim.name)

    # o TRUNCATE fez parte da transação que sofreu rollback
    assert query(pg, f"SELECT a, b FROM {SCHEMA}.atomica") == [("1", "2")]


def test_view_dependente_sobrevive_ao_refresh(pg, tmp_path):
    path = bronze(tmp_path, "a\n1\n")
    pg.copy_csv(path, "comview", schema=SCHEMA, filename=path.name)

    conn = pg.connect()
    with conn.cursor() as cur:
        cur.execute(f"CREATE VIEW {SCHEMA}.v AS SELECT a FROM {SCHEMA}.comview")
    conn.commit()
    conn.close()

    bronze(tmp_path, "a\n2\n")
    pg.copy_csv(path, "comview", schema=SCHEMA, filename=path.name)
    assert query(pg, f"SELECT a FROM {SCHEMA}.v") == [("2",)]


# ------------------------ drift de colunas ------------------------


def test_coluna_nova_no_bronze_e_adicionada(pg, tmp_path):
    pg.copy_csv(bronze(tmp_path, "a\n1\n"), "drift", schema=SCHEMA)
    pg.copy_csv(bronze(tmp_path, "a;nova\n1;2\n"), "drift", schema=SCHEMA)
    assert query(pg, f"SELECT a, nova FROM {SCHEMA}.drift") == [("1", "2")]


def test_coluna_ausente_no_bronze_fica_nula(pg, tmp_path):
    pg.copy_csv(bronze(tmp_path, "a;b\n1;2\n"), "sumiu", schema=SCHEMA)
    pg.copy_csv(bronze(tmp_path, "a\n3\n"), "sumiu", schema=SCHEMA)
    assert query(pg, f"SELECT a, b FROM {SCHEMA}.sumiu") == [("3", None)]


def test_ordem_das_colunas_do_bronze_nao_importa(pg, tmp_path):
    pg.copy_csv(bronze(tmp_path, "a;b\n1;2\n"), "ordem", schema=SCHEMA)
    pg.copy_csv(bronze(tmp_path, "b;a\n20;10\n"), "ordem", schema=SCHEMA)
    assert query(pg, f"SELECT a, b FROM {SCHEMA}.ordem") == [("10", "20")]


# ------------------------ tabela anterior ao carimbo ------------------------


def test_tabela_sem_carimbo_ganha_a_coluna_sem_recreate(pg, tmp_path):
    """Tabela criada antes do carimbo (JSONB da NHL, apsystem, openweather).

    O ``ADD COLUMN`` é de catálogo: as linhas que já estavam lá continuam lá e o
    OID não muda, então as views do dbt sobre a raw sobrevivem.
    """
    conn = pg.connect()
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {SCHEMA}.sem_carimbo (a TEXT)")
        cur.execute(f"INSERT INTO {SCHEMA}.sem_carimbo (a) VALUES ('velha')")
    conn.commit()
    conn.close()
    oid = query(pg, f"SELECT '{SCHEMA}.sem_carimbo'::regclass::oid")

    pg.copy_csv(
        bronze(tmp_path, "a\nnova\n"), "sem_carimbo", schema=SCHEMA, write="append"
    )

    colunas = query(
        pg,
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{SCHEMA}' AND table_name = 'sem_carimbo'",
    )
    assert {c[0] for c in colunas} == {"a", "loaded_at_utc"}
    assert query(
        pg,
        "SELECT count(*), count(*) FILTER (WHERE loaded_at_utc IS NULL) "
        f"FROM {SCHEMA}.sem_carimbo",
    ) == [(2, 0)]
    assert query(pg, f"SELECT '{SCHEMA}.sem_carimbo'::regclass::oid") == oid


def test_carimbo_e_utc_mesmo_com_a_sessao_em_outro_fuso(pg, tmp_path):
    """É o DEFAULT que garante UTC, não o TimeZone do servidor."""
    conn = pg.connect()
    with conn.cursor() as cur:
        cur.execute("SET SESSION TIME ZONE 'America/Sao_Paulo'")
    cliente = PostgresClient(connection=conn)
    cliente.copy_csv(bronze(tmp_path, "a\n1\n"), "fuso", schema=SCHEMA)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT abs(extract(epoch FROM"
            " loaded_at_utc - (now() AT TIME ZONE 'utc')))"
            f" FROM {SCHEMA}.fuso"
        )
        distancia = cur.fetchone()[0]
    conn.close()
    # em São Paulo (UTC-3) o now() puro daria ~10800s de diferença
    assert distancia < 60


def test_jsonb_loader_grava_carimbo(pg, tmp_path):
    from core.jsonb import JsonbLoader

    (tmp_path / "x.json").write_text('[{"a": 1}, {"a": 2}]', encoding="utf-8")
    JsonbLoader(pg, schema=SCHEMA, control_table="ctl").load_files(
        [tmp_path / "x.json"], "jsonb_t"
    )
    assert query(
        pg,
        "SELECT count(*), count(*) FILTER (WHERE loaded_at_utc IS NULL),"
        f" count(DISTINCT loaded_at_utc) FROM {SCHEMA}.jsonb_t",
    ) == [(2, 0, 1)]


# ------------------------ controle e bronze-delta ------------------------


def _cfg_incremental(tmp_path, tabela):
    from core.config import PipelineConfig

    return PipelineConfig(
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
        bronze_file="delta.csv",
        db_schema=SCHEMA,
        db_table=tabela,
        write="append",
        options={"control_table": "ctl"},
    )


def _landing(cfg, nomes):
    for nome in nomes:
        (cfg.landing_dir / f"{nome}.json").write_text("{}", encoding="utf-8")
    return sorted(cfg.landing_dir.glob("*.json"))


def test_ciclo_incremental_carrega_so_o_delta(pg, tmp_path, monkeypatch):
    from core.control import write_bronze_incremental
    from core.etl import GenericETL

    monkeypatch.setattr("core.control.PostgresClient", lambda log=None: pg)
    monkeypatch.setattr("core.etl.PostgresClient", lambda log=None: pg)

    cfg = _cfg_incremental(tmp_path, "incremental")
    parse = lambda f: pd.DataFrame({"x": [f.stem]})  # noqa: E731

    write_bronze_incremental(cfg, _landing(cfg, ["a", "b"]), parse)
    GenericETL(cfg).load()
    assert query(pg, f"SELECT x FROM {SCHEMA}.incremental ORDER BY x") == [
        ("a",),
        ("b",),
    ]

    # chega um arquivo novo: só ele entra
    write_bronze_incremental(cfg, _landing(cfg, ["a", "b", "c"]), parse)
    GenericETL(cfg).load()
    assert query(pg, f"SELECT x FROM {SCHEMA}.incremental ORDER BY x") == [
        ("a",),
        ("b",),
        ("c",),
    ]

    # rodar de novo sem novidade não duplica nada
    write_bronze_incremental(cfg, _landing(cfg, ["a", "b", "c"]), parse)
    GenericETL(cfg).load()
    assert query(pg, f"SELECT count(*) FROM {SCHEMA}.incremental") == [(3,)]


def test_falha_no_load_nao_registra_no_controle(pg, tmp_path, monkeypatch):
    """Registro e COPY estão na mesma transação: ou entram juntos, ou nenhum."""
    from core.control import write_bronze_incremental
    from core.etl import GenericETL

    monkeypatch.setattr("core.control.PostgresClient", lambda log=None: pg)
    monkeypatch.setattr("core.etl.PostgresClient", lambda log=None: pg)

    cfg = _cfg_incremental(tmp_path, "rollback")
    write_bronze_incremental(
        cfg, _landing(cfg, ["a"]), lambda f: pd.DataFrame({"x": [f.stem]})
    )
    # bronze corrompido depois do transform: o COPY falha
    cfg.bronze_filepath.write_text("x\n1;2\n", encoding="utf-8")

    with pytest.raises(psycopg2.Error):
        GenericETL(cfg).load()

    registrados = query(
        pg,
        f"SELECT count(*) FROM {SCHEMA}.ctl WHERE table_name = 'rollback'",
    )
    assert registrados == [(0,)]


def test_load_sem_manifesto_avisa_e_nao_carrega(pg, tmp_path, monkeypatch):
    from core.etl import GenericETL

    monkeypatch.setattr("core.control.PostgresClient", lambda log=None: pg)
    monkeypatch.setattr("core.etl.PostgresClient", lambda log=None: pg)

    cfg = _cfg_incremental(tmp_path, "semmanifesto")
    cfg.bronze_filepath.parent.mkdir(parents=True, exist_ok=True)
    cfg.bronze_filepath.write_text("x\n1\n", encoding="utf-8")

    GenericETL(cfg).load()  # não levanta, mas também não cria a tabela
    assert query(
        pg,
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = '{SCHEMA}' AND table_name = 'semmanifesto'",
    ) == [(0,)]
