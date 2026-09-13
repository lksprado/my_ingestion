import json
from pathlib import Path

from core import PipelineConfig
from pipelines.legislativo.camara import camara_etl


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
    df = camara_etl.parse_dados_abertos(p, url_col="url_votos")
    assert list(df.columns) == ["a", "b.c", "url_votos"]
    assert df["url_votos"].iloc[0] == "http://x/1/votos"


def test_parse_dados_abertos_empty_returns_none(tmp_path):
    p = _write(tmp_path / "1.json", {"dados": [], "links": []})
    assert camara_etl.parse_dados_abertos(p, url_col="u") is None


class FakeHttp:
    def __init__(self, responses: dict):
        self.responses = responses
        self.saved = []

    def get_json(self, url):
        return self.responses[url]

    def save_json(self, data, output_dir, filename):
        self.saved.append(filename)


def test_extract_ids_treats_empty_dados_as_no_data(tmp_path):
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        parameter_dir=tmp_path / "params",
        url_base="http://api/{id}/votos",
        landing_file="{id}_votos.json",
        parameter_file="ids.csv",
        options={"no_data_file": "sem_dados.csv"},
    )
    cfg.parameter_filepath.write_text("id\n1\n2\n")
    http = FakeHttp(
        {
            "http://api/1/votos": {"dados": [{"x": 1}]},
            "http://api/2/votos": {"dados": []},
        }
    )
    camara_etl.extract_ids(cfg, http=http)
    assert http.saved == ["1_votos.json"]
    assert (cfg.parameter_dir / "sem_dados.csv").read_text().split() == ["id", "2"]
