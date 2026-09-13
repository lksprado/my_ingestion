import json
from pathlib import Path

import pandas as pd

from core import PipelineConfig
from pipelines.legislativo import _common


def _write(path: Path, obj) -> Path:
    path.write_text(json.dumps(obj), encoding="utf-8")
    return path


def test_parse_dados_abertos_adds_self_url(tmp_path):
    p = _write(
        tmp_path / "1_votos.json",
        {
            "dados": [{"a": 1, "b": {"c": 2}}],
            "links": [{"rel": "self", "href": "http://x/1/votos"}],
        },
    )
    df = _common.parse_dados_abertos(p, url_col="url_votos")
    assert list(df.columns) == ["a", "b.c", "url_votos"]
    assert df["url_votos"].iloc[0] == "http://x/1/votos"


def test_parse_dados_abertos_empty_returns_none(tmp_path):
    p = _write(tmp_path / "1.json", {"dados": [], "links": []})
    assert _common.parse_dados_abertos(p, url_col="u") is None


def test_read_ids_coerces_floats_and_dedups(tmp_path):
    csv = tmp_path / "ids.csv"
    csv.write_text("idprocesso\n123.0\n123.0\n\n456.0\n")
    assert _common.read_ids(csv, "idprocesso") == ["123", "456"]


def test_landing_ids_strips_suffix(tmp_path):
    (tmp_path / "10_votos_deputados.json").write_text("{}")
    (tmp_path / "20_votos_deputados.json").write_text("{}")
    (tmp_path / "ignorar.csv").write_text("")
    assert _common.landing_ids(tmp_path, "_votos_deputados") == {"10", "20"}


class FakeHttp:
    def __init__(self, responses: dict):
        self.responses = responses
        self.saved = []

    def get_json(self, url):
        return self.responses[url]

    def save_json(self, data, output_dir, filename):
        self.saved.append(filename)
        (Path(output_dir) / filename).write_text(json.dumps(data))


def _cfg(tmp_path, **options) -> PipelineConfig:
    return PipelineConfig(
        landing_dir=tmp_path / "landing",
        parameter_dir=tmp_path / "params",
        url_base="http://api/{id}/votos",
        landing_file="{id}_votos.json",
        parameter_file="ids.csv",
        options={"no_data_file": "sem_dados.csv", **options},
    )


def test_extract_by_ids_skips_done_and_blacklists(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.parameter_filepath.write_text("id\n1\n2\n3\n4\n")
    (cfg.landing_dir / "1_votos.json").write_text("{}")  # já baixado
    http = FakeHttp(
        {
            "http://api/2/votos": {"dados": [{"x": 1}]},
            "http://api/3/votos": {"dados": []},  # sem dados
            "http://api/4/votos": None,  # timeout
        }
    )
    _common.extract_by_ids(cfg, http=http)

    assert http.saved == ["2_votos.json"]
    no_data = (cfg.parameter_dir / "sem_dados.csv").read_text().split()
    assert no_data == ["id", "3", "4"]

    # Segunda rodada: nada pendente, nenhuma requisição.
    http2 = FakeHttp({})
    _common.extract_by_ids(cfg, http=http2)
    assert http2.saved == []


def test_extract_by_ids_without_blacklist_on_error(tmp_path):
    cfg = _cfg(tmp_path, blacklist_on_error=False)
    cfg.parameter_filepath.write_text("id\n4\n")
    _common.extract_by_ids(cfg, http=FakeHttp({"http://api/4/votos": None}))
    assert not (cfg.parameter_dir / "sem_dados.csv").exists()


def test_extract_by_ids_custom_has_data(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.parameter_filepath.write_text("id\n7\n")
    http = FakeHttp({"http://api/7/votos": [{"raiz": "array"}]})
    _common.extract_by_ids(cfg, has_data=bool, http=http)
    assert http.saved == ["7_votos.json"]


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

    assert _common.concat_landing(cfg, parse)["x"].tolist() == [1]
    assert _common.concat_landing(cfg, lambda f: None).empty


def test_flatten_children():
    records = [
        {"id": 1, "nome": "a", "votos": [{"v": "sim"}, {"v": "nao"}]},
        {"id": 2, "nome": "b", "votos": None},
    ]
    rows = _common.flatten_children(records, ["id"], "votos")
    assert rows == [{"id": 1, "v": "sim"}, {"id": 1, "v": "nao"}]
