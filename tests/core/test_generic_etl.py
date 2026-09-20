from pathlib import Path

import pandas as pd
import pytest

import core.etl as etl_module
from core.config import PipelineConfig
from core.etl import Etl, GenericETL, build_etl, run_source


def _cfg(tmp_path: Path, **kw) -> PipelineConfig:
    base = dict(landing_dir=tmp_path / "ld", bronze_dir=tmp_path / "brz")
    return PipelineConfig(**{**base, **kw})


def test_default_extract_downloads_url_base(monkeypatch, tmp_path):
    called = {}

    class FakeHttp:
        def __init__(self, logger): ...

        def fetch_and_save(self, url, output_dir, filename):
            called.update(url=url, output_dir=Path(output_dir), filename=filename)

    monkeypatch.setattr(etl_module, "HttpClient", FakeHttp)
    cfg = _cfg(tmp_path, url_base="https://api.example/test", landing_file="d.json")
    GenericETL(cfg).extract()
    assert called == {
        "url": "https://api.example/test",
        "output_dir": cfg.landing_dir,
        "filename": "d.json",
    }


def test_default_extract_without_url_base_is_noop(monkeypatch, tmp_path):
    monkeypatch.setattr(
        etl_module, "HttpClient", lambda *_: pytest.fail("não deveria requisitar")
    )
    cfg = _cfg(tmp_path)
    assert GenericETL(cfg).extract() == cfg.landing_dir


def test_custom_functions_receive_cfg(tmp_path):
    seen = []
    cfg = _cfg(tmp_path)
    etl = GenericETL(
        cfg,
        extract_fn=lambda c: seen.append(("e", c)),
        transform_fn=lambda c: seen.append(("t", c)),
        load_fn=lambda c: seen.append(("l", c)),
    )
    etl.run()
    assert [s for s, _ in seen] == ["e", "t", "l"]
    assert all(c is cfg for _, c in seen)


def test_transform_without_function_is_noop(tmp_path):
    GenericETL(_cfg(tmp_path)).transform()  # não levanta


def test_run_subset_of_steps_in_order(tmp_path):
    seen = []
    etl = GenericETL(
        _cfg(tmp_path),
        extract_fn=lambda c: seen.append("e"),
        transform_fn=lambda c: seen.append("t"),
        load_fn=lambda c: seen.append("l"),
    )
    etl.run(steps=["load", "transform"])
    assert seen == ["t", "l"]


def test_run_rejects_unknown_step(tmp_path):
    with pytest.raises(ValueError, match="inválida"):
        GenericETL(_cfg(tmp_path)).run(steps=["extrair"])


class _FakeCopy:
    """PostgresClient falso que só registra a chamada de COPY."""

    chamadas: list = []

    def __init__(self, log=None): ...

    def copy_csv(
        self,
        path,
        table_name,
        *,
        schema,
        sep,
        write,
        filename,
        merge_key=None,
        after_copy=None,
    ):
        _FakeCopy.chamadas.append(
            {
                "path": Path(path),
                "table": table_name,
                "schema": schema,
                "sep": sep,
                "write": write,
                "filename": filename,
                "merge_key": merge_key,
            }
        )


def test_load_table_manda_o_bronze_inteiro_num_unico_copy(monkeypatch, tmp_path):
    _FakeCopy.chamadas = []
    monkeypatch.setattr(etl_module, "PostgresClient", _FakeCopy)

    cfg = _cfg(tmp_path, bronze_file="f.csv", db_table="t", db_schema="raw_x")
    pd.DataFrame({"x": [1, 2, 3]}).to_csv(cfg.bronze_filepath, sep=";", index=False)
    GenericETL(cfg).load()

    assert _FakeCopy.chamadas == [
        {
            "path": cfg.bronze_filepath,
            "table": "t",
            "schema": "raw_x",
            "sep": ";",
            "write": "truncate",
            "filename": "f.csv",
            "merge_key": None,
        }
    ]


def test_load_table_usa_o_write_do_yaml(monkeypatch, tmp_path):
    _FakeCopy.chamadas = []
    monkeypatch.setattr(etl_module, "PostgresClient", _FakeCopy)

    cfg = _cfg(
        tmp_path, bronze_file="f.csv", db_table="t", db_schema="raw_x", write="append"
    )
    cfg.bronze_filepath.write_text("a;b\n1;2\n")
    GenericETL(cfg).load()

    assert _FakeCopy.chamadas[0]["write"] == "append"


def test_load_table_repassa_a_merge_key_do_yaml(monkeypatch, tmp_path):
    _FakeCopy.chamadas = []
    monkeypatch.setattr(etl_module, "PostgresClient", _FakeCopy)

    cfg = _cfg(
        tmp_path,
        bronze_file="f.csv",
        db_table="t",
        db_schema="raw_x",
        write="merge",
        merge_key=["date"],
    )
    cfg.bronze_filepath.write_text("date;v\n2026-01-01;1\n")
    GenericETL(cfg).load()

    assert _FakeCopy.chamadas[0]["write"] == "merge"
    assert _FakeCopy.chamadas[0]["merge_key"] == ["date"]


def test_load_table_com_so_cabecalho_nao_tem_caso_especial(monkeypatch, tmp_path):
    # O COPY de um CSV só com cabeçalho cria a tabela e insere 0 linhas: o
    # branch que existia para isso no _load_table deixou de ser necessário.
    _FakeCopy.chamadas = []
    monkeypatch.setattr(etl_module, "PostgresClient", _FakeCopy)

    cfg = _cfg(tmp_path, bronze_file="f.csv", db_table="t", db_schema="raw_x")
    cfg.bronze_filepath.write_text("a;b\n")
    GenericETL(cfg).load()

    assert len(_FakeCopy.chamadas) == 1


def test_load_requires_raw_schema(monkeypatch, tmp_path):
    monkeypatch.setattr(
        etl_module, "PostgresClient", lambda **_: pytest.fail("não deveria conectar")
    )
    cfg = _cfg(tmp_path, bronze_file="f.csv", db_table="t")
    with pytest.raises(ValueError, match="raw_<fonte>"):
        GenericETL(cfg).load()


def test_load_none_does_not_connect(monkeypatch, tmp_path):
    monkeypatch.setattr(
        etl_module, "PostgresClient", lambda **_: pytest.fail("não deveria conectar")
    )
    GenericETL(_cfg(tmp_path, load="none")).load()


def test_load_files_uses_bronze_dir_and_sep(monkeypatch, tmp_path):
    called = {}

    class FakePg:
        def __init__(self, log=None): ...

        def load_files_to_table(
            self, input_dir, *, schema, pattern, write, sep, merge_key=None
        ):
            called.update(
                input_dir=Path(input_dir),
                schema=schema,
                pattern=pattern,
                write=write,
                sep=sep,
                merge_key=merge_key,
            )

    monkeypatch.setattr(etl_module, "PostgresClient", FakePg)
    cfg = _cfg(tmp_path, load="files", db_schema="raw_b3", bronze_sep=",")
    GenericETL(cfg).load()
    assert called == {
        "input_dir": cfg.bronze_dir,
        "schema": "raw_b3",
        "pattern": "*.csv",
        "write": "truncate",
        "sep": ",",
        "merge_key": None,
    }


def test_load_jsonb_passes_options(monkeypatch, tmp_path):
    called = {}

    class FakePg:
        def __init__(self, log=None): ...

    class FakeLoader:
        def __init__(self, db, *, schema, control_table, log=None):
            called.update(schema=schema, control_table=control_table)

        def load_files(self, files, table, array_key=None, overwrite=False):
            called.update(
                files=[f.name for f in files],
                table=table,
                array_key=array_key,
                overwrite=overwrite,
            )

    monkeypatch.setattr(etl_module, "PostgresClient", FakePg)
    monkeypatch.setattr(etl_module, "JsonbLoader", FakeLoader)

    cfg = _cfg(
        tmp_path,
        load="jsonb",
        db_schema="raw_nhl",
        db_table="nhl_raw_teams",
        landing_file="teams.json",
        options={"array_key": "data", "overwrite": True, "control_table": "ctl"},
    )
    (cfg.landing_dir / "teams.json").write_text("{}")
    GenericETL(cfg).load()
    assert called == {
        "schema": "raw_nhl",
        "control_table": "ctl",
        "files": ["teams.json"],
        "table": "nhl_raw_teams",
        "array_key": "data",
        "overwrite": True,
    }


YAML = """
db_schema: raw_teste
environments:
  dev:
    base_raw: "{root}/raw"
    base_bronze: "{root}/bronze"
sources:
  a:
    subpath: a
  b:
    subpath: b
    load: none
"""


@pytest.fixture
def yml(tmp_path: Path) -> Path:
    p = tmp_path / "teste_config.yml"
    p.write_text(YAML.format(root=tmp_path))
    return p


def _recording(seen: list, name: str) -> Etl:
    return Etl(
        extract=lambda c: seen.append(f"{name}.e"),
        transform=lambda c: seen.append(f"{name}.t"),
        load=lambda c: seen.append(f"{name}.l"),
    )


def test_build_etl_wires_yaml_and_functions(yml):
    etl = Etl(transform=lambda c: None)
    built = build_etl(yml, "b", etl)
    assert built.cfg.load == "none"
    assert built.cfg.db_schema == "raw_teste"
    assert built.extract_fn is None
    assert built.transform_fn is etl.transform


def test_run_source_runs_all_in_order_by_default(yml):
    seen = []
    etls = {"b": _recording(seen, "b"), "a": _recording(seen, "a")}
    run_source(yml, etls, argv=["--steps", "transform,load"])
    assert seen == ["b.t", "b.l", "a.t", "a.l"]


def test_run_source_only_selected_entities(yml):
    seen = []
    etls = {"a": _recording(seen, "a"), "b": _recording(seen, "b")}
    run_source(yml, etls, argv=["b", "--steps", "extract"])
    assert seen == ["b.e"]


@pytest.mark.parametrize("argv", [["zzz"], ["--steps", "tranform"]])
def test_run_source_rejects_unknown_entity_or_step(yml, argv):
    with pytest.raises(SystemExit):
        run_source(yml, {"a": Etl()}, argv=argv)


def test_run_source_isolates_failures_and_exits_1(yml):
    seen = []

    def boom(cfg):
        raise RuntimeError("falhou")

    etls = {"a": Etl(extract=boom), "b": _recording(seen, "b")}
    with pytest.raises(SystemExit) as exc:
        run_source(yml, etls, argv=["--steps", "extract"])
    assert exc.value.code == 1
    assert seen == ["b.e"]


def test_run_source_single_entity_propagates_error(yml):
    def boom(cfg):
        raise RuntimeError("falhou")

    with pytest.raises(RuntimeError):
        run_source(yml, {"a": Etl(extract=boom)}, argv=["a", "--steps", "extract"])
