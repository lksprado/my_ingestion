import copy
import importlib.util
from pathlib import Path

import pytest
import yaml

from core.config import load_yaml, validate_config

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_CONFIGS = sorted(
    p
    for p in (REPO_ROOT / "src" / "pipelines").rglob("*_config.yml")
    if "sources" in (load_yaml(p) or {})
)

_spec = importlib.util.spec_from_file_location(
    "validar_configs", REPO_ROOT / "scripts" / "validar_configs.py"
)
validar_configs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(validar_configs)

BASE = {
    "db_schema": "raw_exemplo",
    "environments": {
        "dev": {"base_raw": "${LAKE_ROOT}/raw/exemplo"},
        "prod": {"base_raw": "/usr/local/airflow/mylake/raw/exemplo"},
    },
    "sources": {
        "itens": {
            "base_url": "https://api.exemplo/v1/itens",
            "landing_file": "itens_{date}.json",
            "db_table": "itens",
        }
    },
}


def _cfg(**changes) -> dict:
    cfg = copy.deepcopy(BASE)
    for path, value in changes.items():
        *parents, leaf = path.split("__")
        node = cfg
        for key in parents:
            node = node[key]
        if value is None:
            node.pop(leaf)
        else:
            node[leaf] = value
    return cfg


@pytest.mark.parametrize(
    "config_file", PIPELINE_CONFIGS, ids=lambda p: p.relative_to(REPO_ROOT).as_posix()
)
def test_configs_reais_sao_validos(config_file: Path):
    assert validar_configs.validate_file(config_file) == []


def test_config_minimo_valido():
    assert validate_config(BASE) == []


@pytest.mark.parametrize(
    ("changes", "esperado"),
    [
        (
            {"sources__itens__db_tabel": "x"},
            "sources.itens.db_tabel: chave desconhecida",
        ),
        ({"dbschema": "raw_x"}, "dbschema: chave desconhecida"),
        ({"sources__itens__load": "tabel"}, "sources.itens.load: 'tabel' inválido"),
        ({"write": "upsert"}, "sources.itens.write: 'upsert' inválido"),
        ({"environments__prod": None}, "environments.prod: ambiente faltando"),
        ({"environments__hml": {"base_raw": "x"}}, "environments.hml: ambiente"),
        ({"environments__dev__base_raw": None}, "environments.dev.base_raw: obrig"),
        ({"environments__dev__base_raw": "/home/x"}, "environments.dev.base_raw: use"),
        ({"db_schema": "public"}, "sources.itens.db_schema: load=table exige"),
        ({"sources__itens__db_table": None}, "sources.itens.db_table: obrigatório"),
        ({"bronze_sep": ";;"}, "sources.itens.bronze_sep"),
        (
            {"sources__itens__output_param_file": ["a"]},
            "sources.itens.output_param_file",
        ),
        ({"options": ["a"]}, "options: deve ser um mapeamento"),
        ({"sources": {}}, "sources: obrigatório"),
        ({"write": "append"}, "sources.itens.write: append exige"),
        (
            {"write": "append", "options": {"control_tabel": "c"}},
            "sources.itens.write: append exige",
        ),
    ],
)
def test_erros_de_estrutura(changes, esperado):
    errors = validate_config(_cfg(**changes))
    assert any(e.startswith(esperado) for e in errors), errors


def test_source_sobrescreve_o_topo():
    cfg = _cfg(
        write="upsert",
        sources__itens__write="append",
        sources__itens__options={"control_table": "controle"},
    )
    assert validate_config(cfg) == []


def test_append_aceita_control_table_do_topo():
    cfg = _cfg(write="append", options={"control_table": "controle"})
    assert validate_config(cfg) == []


def test_append_sem_controle_so_importa_no_load_table():
    # jsonb tem controle próprio (JsonbLoader) e não lê o write.
    assert validate_config(_cfg(write="append", load="jsonb")) == []


def test_load_none_dispensa_schema_e_tabela():
    cfg = _cfg(db_schema=None, sources__itens__db_table=None)
    cfg["sources"]["itens"]["load"] = "none"
    assert validate_config(cfg) == []


def test_load_files_exige_schema_mas_nao_tabela():
    cfg = _cfg(load="files", sources__itens__db_table=None)
    assert validate_config(cfg) == []
    assert validate_config({**cfg, "db_schema": None})


def test_sources_batem_com_etls(tmp_path: Path):
    (tmp_path / "exemplo_etl.py").write_text(
        'ETLS = {"itens": Etl(), "orfao": Etl()}\n'
        'cfg = PipelineConfig.from_yaml(CONFIG_FILE, "avulsa")\n',
        encoding="utf-8",
    )
    cfg = copy.deepcopy(BASE)
    cfg["sources"]["avulsa"] = {"load": "none"}
    cfg["sources"]["sem_etl"] = {"load": "none"}
    config_file = tmp_path / "exemplo_config.yml"
    config_file.write_text(yaml.safe_dump(cfg), encoding="utf-8")

    assert validar_configs.validate_file(config_file) == [
        "sources.sem_etl: sem entrada em ETLS do *_etl.py",
        "ETLS['orfao']: sem source correspondente no YAML",
    ]


def test_yaml_sem_sources_e_pulado(tmp_path: Path):
    config_file = tmp_path / "store_config.yml"
    config_file.write_text("stores:\n  - id: x\n", encoding="utf-8")
    assert validar_configs.validate_file(config_file) == []
