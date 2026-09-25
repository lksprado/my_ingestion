import json
from pathlib import Path

import pandas as pd
import pytest

from core.config import PipelineConfig
from core.io import (
    concat_landing,
    integral_floats_to_int,
    reset_bronze,
    write_bronze,
    write_bronze_streaming,
)


@pytest.fixture
def cfg(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        landing_dir=tmp_path / "ld", bronze_dir=tmp_path / "brz", bronze_file="b.csv"
    )


def test_write_bronze_sanitizes_and_strips_newlines(cfg):
    df = pd.DataFrame({"Nome Ação": ["a\nb", "c\r\nd"], "Valor": [1, 2]})
    path = write_bronze(cfg, df)
    assert path == cfg.bronze_filepath
    out = pd.read_csv(path, sep=";")
    assert list(out.columns) == ["nome_acao", "valor"]
    assert out["nome_acao"].tolist() == ["a b", "c d"]


def test_write_bronze_respects_sep(tmp_path):
    cfg = PipelineConfig(
        landing_dir=tmp_path, bronze_dir=tmp_path, bronze_file="b.csv", bronze_sep=","
    )
    write_bronze(cfg, pd.DataFrame({"a": [1], "b": [2]}))
    assert cfg.bronze_filepath.read_text() == "a,b\n1,2\n"


def test_write_bronze_empty_preserves_previous(cfg):
    cfg.bronze_filepath.write_text("x;y\n1;2\n")
    assert write_bronze(cfg, pd.DataFrame()) is None
    assert write_bronze(cfg, None) is None
    assert cfg.bronze_filepath.read_text() == "x;y\n1;2\n"


def _files(tmp_path: Path, n: int) -> list[Path]:
    files = []
    for i in range(n):
        f = tmp_path / f"f{i}.json"
        f.write_text("{}")
        files.append(f)
    return files


def test_streaming_fixes_header_on_first_and_reindexes(cfg, tmp_path):
    files = _files(tmp_path, 3)
    frames = {
        "f0": pd.DataFrame({"A": [1], "B": ["x\ny"]}),
        "f1": pd.DataFrame({"B": ["z"], "A": [2], "extra": [9]}),
        "f2": pd.DataFrame({"A": [3]}),
    }
    path = write_bronze_streaming(cfg, files, lambda f: frames[f.stem])
    out = pd.read_csv(path, sep=";")
    assert list(out.columns) == ["a", "b"]
    assert out["a"].tolist() == [1, 2, 3]
    assert out["b"].tolist() == ["x y", "z", None] or out["b"].isna().iloc[2]
    assert not list(tmp_path.glob("brz/.b_*.tmp"))


def test_streaming_skips_none_and_errors(cfg, tmp_path):
    files = _files(tmp_path, 3)

    def parse(f):
        if f.stem == "f0":
            return None
        if f.stem == "f1":
            raise RuntimeError("boom")
        return pd.DataFrame({"a": [1]})

    path = write_bronze_streaming(cfg, files, parse)
    assert pd.read_csv(path, sep=";")["a"].tolist() == [1]


def test_streaming_without_data_preserves_previous(cfg, tmp_path):
    cfg.bronze_filepath.write_text("x;y\n1;2\n")
    assert write_bronze_streaming(cfg, _files(tmp_path, 2), lambda f: None) is None
    assert cfg.bronze_filepath.read_text() == "x;y\n1;2\n"
    assert not list(cfg.bronze_dir.glob(".b_*.tmp"))


def _parse_json_frame(f: Path) -> pd.DataFrame | None:
    """Parser picklável para o pool: o JSON do arquivo vira o DataFrame."""
    data = json.loads(f.read_text())
    if data == "erro":
        raise RuntimeError("boom")
    return pd.DataFrame(data) if data else None


def test_streaming_with_workers_matches_sequential(tmp_path):
    conteudos = [
        {"A": [1, 2], "B": ["x\ny", None]},
        None,
        "erro",
        {"B": ["z"], "A": [3], "extra": [9]},
        {"A": [4.0]},
    ] * 3
    files = []
    for i, c in enumerate(conteudos):
        f = tmp_path / f"f{i:02d}.json"
        f.write_text(json.dumps(c))
        files.append(f)

    saidas = []
    for workers in (1, 2):
        cfg = PipelineConfig(
            landing_dir=tmp_path,
            bronze_dir=tmp_path / f"brz{workers}",
            bronze_file="b.csv",
        )
        write_bronze_streaming(cfg, files, _parse_json_frame, workers=workers)
        saidas.append(cfg.bronze_filepath.read_text())
    assert saidas[0] == saidas[1]
    assert saidas[0].splitlines()[:3] == ["a;b", "1;x y", "2;"]


def test_streaming_workers_from_options(tmp_path, monkeypatch):
    usados = []
    import core.io as cio

    original = cio._prepared_frames

    def espiao(files, parse_fn, workers):
        usados.append(workers)
        return original(files, parse_fn, workers)

    monkeypatch.setattr(cio, "_prepared_frames", espiao)
    cfg = PipelineConfig(
        landing_dir=tmp_path,
        bronze_dir=tmp_path / "brz",
        bronze_file="b.csv",
        options={"transform_workers": 3},
    )
    write_bronze_streaming(cfg, [], _parse_json_frame)
    write_bronze_streaming(cfg, [], _parse_json_frame, workers=1)
    assert usados == [3, 1]


def test_concat_landing_skips_errors_and_empty(tmp_path):
    cfg = PipelineConfig(landing_dir=tmp_path)
    for name in ("a", "b", "c"):
        (tmp_path / f"{name}.json").write_text("{}")

    def parse(f):
        if f.stem == "a":
            raise ValueError("boom")
        if f.stem == "b":
            return pd.DataFrame()
        return pd.DataFrame({"x": [1]})

    assert concat_landing(cfg, parse)["x"].tolist() == [1]
    assert concat_landing(cfg, lambda f: None).empty


def test_reset_bronze_removes_only_csvs(tmp_path):
    cfg = PipelineConfig(landing_dir=tmp_path / "ld", bronze_dir=tmp_path)
    (tmp_path / "velha.csv").write_text("a\n1\n")
    (tmp_path / "manter.txt").write_text("x")
    reset_bronze(cfg)
    assert sorted(f.name for f in tmp_path.iterdir() if f.is_file()) == ["manter.txt"]


def test_integral_floats_to_int_keeps_real_floats():
    df = pd.DataFrame(
        {
            "id": [123.0, None, 7.0],
            "preco": [1.5, 2.0, None],
            "texto": ["a", "b", "c"],
            "nulos": [None, None, None],
        }
    )
    out = integral_floats_to_int(df)
    assert out["id"].tolist()[0] == 123 and str(out["id"].dtype) == "Int64"
    assert out["preco"].dtype == float
    assert out["texto"].tolist() == ["a", "b", "c"]
    assert df["id"].dtype == float  # não altera o original


def test_write_bronze_writes_integers_without_decimal(cfg):
    write_bronze(cfg, pd.DataFrame({"id": [123.0, None], "v": [1.5, 2.0]}))
    assert cfg.bronze_filepath.read_text().splitlines() == ["id;v", "123;1.5", ";2.0"]
