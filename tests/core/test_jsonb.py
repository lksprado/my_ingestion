import json
from pathlib import Path

import pytest

from core.jsonb import json_file_to_ndjson_buffer


def _write(tmp_path: Path, obj, name="f.json") -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(obj), encoding="utf-8")
    return p


def _lines(buf) -> list[str]:
    return buf.read().splitlines()


def test_list_becomes_one_line_per_item(tmp_path):
    p = _write(tmp_path, [{"a": 1}, {"a": 2}])
    lines = _lines(json_file_to_ndjson_buffer(p))
    assert lines == ['{"a":1}\tf.json', '{"a":2}\tf.json']


def test_dict_with_array_key(tmp_path):
    p = _write(tmp_path, {"data": [{"id": 1}], "total": 1})
    lines = _lines(json_file_to_ndjson_buffer(p, array_key="data"))
    assert lines == ['{"id":1}\tf.json']


def test_dict_without_array_key_is_single_record(tmp_path):
    p = _write(tmp_path, {"id": 1, "nested": {"x": [1, 2]}})
    lines = _lines(json_file_to_ndjson_buffer(p, source_filename="src.json"))
    assert len(lines) == 1
    payload, source = lines[0].split("\t")
    assert json.loads(payload) == {"id": 1, "nested": {"x": [1, 2]}}
    assert source == "src.json"


def test_copy_text_escaping(tmp_path):
    p = _write(tmp_path, {"s": "a\tb\nc\\d"})
    line = _lines(json_file_to_ndjson_buffer(p))[0]
    payload = line.split("\t")[0]
    # tab/newline não podem aparecer crus; backslash duplicado
    assert "\t" not in payload and "\n" not in payload
    assert payload == '{"s":"a\\\\tb\\\\nc\\\\\\\\d"}'
    # desfazendo o escape do COPY volta ao JSON original
    unescaped = payload.replace("\\\\", "\\")
    assert json.loads(unescaped) == {"s": "a\tb\nc\\d"}


def test_invalid_json_raises(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON inválido"):
        json_file_to_ndjson_buffer(p)


def test_missing_array_key_raises(tmp_path):
    p = _write(tmp_path, {"data": {"not": "a list"}})
    with pytest.raises(ValueError, match="array_key"):
        json_file_to_ndjson_buffer(p, array_key="data")
