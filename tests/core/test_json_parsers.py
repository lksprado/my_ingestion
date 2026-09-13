import json
from pathlib import Path

from core.parsers.json import flatten_children, normalize_json_object


def test_normalize_json_object_under_key(tmp_path: Path):
    fp = tmp_path / "sample.json"
    fp.write_text(json.dumps({"dados": {"a": 1, "b": {"c": 2}}}), encoding="utf-8")

    df = normalize_json_object(fp, key="dados")
    assert list(df.columns) == ["a", "b.c"]
    assert df.iloc[0].tolist() == [1, 2]


def test_normalize_json_object_missing_key_is_empty(tmp_path: Path):
    fp = tmp_path / "sample.json"
    fp.write_text(json.dumps({"other": []}), encoding="utf-8")
    assert normalize_json_object(fp, key="dados").empty


def test_flatten_children():
    records = [
        {"id": 1, "nome": "a", "votos": [{"v": "sim"}, {"v": "nao"}]},
        {"id": 2, "nome": "b", "votos": None},
    ]
    rows = flatten_children(records, ["id"], "votos")
    assert rows == [{"id": 1, "v": "sim"}, {"id": 1, "v": "nao"}]
