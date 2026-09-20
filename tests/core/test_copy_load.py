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
        "AND column_name NOT IN ('data_carga')",
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


def test_data_carga_e_uma_so_para_a_carga_inteira(pg, tmp_path):
    path = bronze(tmp_path, "a\n" + "\n".join(str(i) for i in range(500)) + "\n")
    pg.copy_csv(path, "rastreio", schema=SCHEMA, filename=path.name)

    assert query(
        pg,
        "SELECT count(DISTINCT data_carga),"
        " count(*) FILTER (WHERE data_carga IS NULL),"
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
    assert {c[0] for c in colunas} == {"a", "data_carga"}


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


# ------------------------ tabelas legadas do to_sql ------------------------


def test_tabela_legada_sem_default_ganha_data_carga(pg, tmp_path):
    """O ``to_sql`` criava ``data_carga`` sem DEFAULT; sem migrar, viria NULL."""
    conn = pg.connect()
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {SCHEMA}.legada "
            "(a TEXT, arquivo_origem TEXT, data_carga TIMESTAMP)"
        )
    conn.commit()
    conn.close()

    path = bronze(tmp_path, "a\n1\n")
    pg.copy_csv(path, "legada", schema=SCHEMA, filename=path.name)

    assert query(
        pg,
        f"SELECT a, arquivo_origem, data_carga IS NOT NULL FROM {SCHEMA}.legada",
    ) == [("1", "f.csv", True)]


# ------------------------ write: merge ------------------------


def test_merge_insere_e_atualiza_pela_chave(pg, tmp_path):
    pg.copy_csv(
        bronze(tmp_path, "date;temp\n2026-01-01;10\n2026-01-02;20\n"),
        "clima",
        schema=SCHEMA,
        write="merge",
        merge_key=["date"],
    )
    pg.copy_csv(
        bronze(tmp_path, "date;temp\n2026-01-02;22\n2026-01-03;30\n"),
        "clima",
        schema=SCHEMA,
        write="merge",
        merge_key=["date"],
    )

    assert query(pg, f"SELECT date, temp FROM {SCHEMA}.clima ORDER BY date") == [
        ("2026-01-01", "10"),  # intocada
        ("2026-01-02", "22"),  # atualizada
        ("2026-01-03", "30"),  # inserida
    ]


def test_merge_cria_indice_unico_da_chave(pg, tmp_path):
    pg.copy_csv(
        bronze(tmp_path, "date;temp\n2026-01-01;10\n"),
        "comindice",
        schema=SCHEMA,
        write="merge",
        merge_key=["date"],
    )
    indices = query(
        pg,
        "SELECT indexdef FROM pg_indexes "
        f"WHERE schemaname = '{SCHEMA}' AND tablename = 'comindice'",
    )
    assert len(indices) == 1
    assert "UNIQUE" in indices[0][0] and "(date)" in indices[0][0]


def test_merge_reaproveita_indice_unico_existente(pg, tmp_path):
    """openweather/solar já têm índice feito à mão; não criar um segundo igual."""
    conn = pg.connect()
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {SCHEMA}.jatinha (date TEXT, temp TEXT)")
        cur.execute(f"CREATE UNIQUE INDEX feito_a_mao ON {SCHEMA}.jatinha (date)")
    conn.commit()
    conn.close()

    pg.copy_csv(
        bronze(tmp_path, "date;temp\n2026-01-01;10\n"),
        "jatinha",
        schema=SCHEMA,
        write="merge",
        merge_key=["date"],
    )
    indices = query(
        pg,
        "SELECT indexname FROM pg_indexes "
        f"WHERE schemaname = '{SCHEMA}' AND tablename = 'jatinha'",
    )
    assert [i[0] for i in indices] == ["feito_a_mao"]


def test_merge_com_chave_composta(pg, tmp_path):
    csv = "url;dep;voto\na;1;sim\na;2;nao\n"
    pg.copy_csv(
        bronze(tmp_path, csv),
        "composta",
        schema=SCHEMA,
        write="merge",
        merge_key=["url", "dep"],
    )
    pg.copy_csv(
        bronze(tmp_path, "url;dep;voto\na;2;abstencao\n"),
        "composta",
        schema=SCHEMA,
        write="merge",
        merge_key=["url", "dep"],
    )
    assert query(pg, f"SELECT url, dep, voto FROM {SCHEMA}.composta ORDER BY dep") == [
        ("a", "1", "sim"),
        ("a", "2", "abstencao"),
    ]


def test_merge_tolera_chave_repetida_no_lote(pg, tmp_path):
    # sem DISTINCT ON o Postgres erra "cannot affect row a second time"
    pg.copy_csv(
        bronze(tmp_path, "date;temp\n2026-01-01;10\n2026-01-01;11\n"),
        "repetida",
        schema=SCHEMA,
        write="merge",
        merge_key=["date"],
    )
    assert query(pg, f"SELECT count(*) FROM {SCHEMA}.repetida") == [(1,)]


def test_merge_atualiza_data_carga_e_arquivo_origem(pg, tmp_path):
    p1 = bronze(tmp_path, "date;temp\n2026-01-01;10\n", nome="a.csv")
    pg.copy_csv(
        p1, "rastro", schema=SCHEMA, write="merge", merge_key=["date"], filename="a.csv"
    )
    antes = query(pg, f"SELECT arquivo_origem, data_carga FROM {SCHEMA}.rastro")

    p2 = bronze(tmp_path, "date;temp\n2026-01-01;11\n", nome="b.csv")
    pg.copy_csv(
        p2, "rastro", schema=SCHEMA, write="merge", merge_key=["date"], filename="b.csv"
    )
    depois = query(pg, f"SELECT arquivo_origem, data_carga FROM {SCHEMA}.rastro")

    assert antes[0][0] == "a.csv" and depois[0][0] == "b.csv"
    assert depois[0][1] > antes[0][1]


def test_merge_em_tabela_com_duplicatas_falha_alto(pg, tmp_path):
    """Criar o índice único sobre dado duplicado tem de quebrar, não passar batido."""
    pg.copy_csv(
        bronze(tmp_path, "date;temp\n2026-01-01;10\n2026-01-01;11\n"),
        "suja",
        schema=SCHEMA,
    )
    with pytest.raises(psycopg2.Error):
        pg.copy_csv(
            bronze(tmp_path, "date;temp\n2026-01-02;20\n"),
            "suja",
            schema=SCHEMA,
            write="merge",
            merge_key=["date"],
        )


def test_jsonb_loader_grava_data_carga(pg, tmp_path):
    from core.jsonb import JsonbLoader

    (tmp_path / "x.json").write_text('[{"a": 1}, {"a": 2}]', encoding="utf-8")
    JsonbLoader(pg, schema=SCHEMA, control_table="ctl").load_files(
        [tmp_path / "x.json"], "jsonb_t"
    )
    assert query(
        pg,
        "SELECT count(*), count(*) FILTER (WHERE data_carga IS NULL),"
        f" count(DISTINCT data_carga) FROM {SCHEMA}.jsonb_t",
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
