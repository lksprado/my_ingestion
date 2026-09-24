import json
from datetime import date
from pathlib import Path

import pandas as pd

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


def test_trimestres_ultimos_atravessa_o_ano():
    got = camara_etl.trimestres(2, hoje=date(2026, 1, 15))
    assert got == [
        (2025, "2025-10-01", "2026-01-01", "Q4"),
        (2026, "2026-01-01", "2026-04-01", "Q1"),
    ]


def test_trimestres_desde_vai_de_janeiro_ao_corrente():
    got = camara_etl.trimestres(desde=2025, hoje=date(2026, 5, 2))
    assert [(y, q) for y, *_, q in got] == [
        (2025, "Q1"),
        (2025, "Q2"),
        (2025, "Q3"),
        (2025, "Q4"),
        (2026, "Q1"),
        (2026, "Q2"),
    ]


class PagedHttp:
    def __init__(self, responses: dict):
        self.responses = responses
        self.saved = {}

    def get_json(self, url):
        return self.responses.get(url)

    def save_json(self, data, output_dir, filename):
        self.saved[filename] = data


def _leg_cfg(tmp_path) -> PipelineConfig:
    return PipelineConfig(
        landing_dir=tmp_path / "landing",
        url_base="http://api/deputados?idLegislatura={legislatura}&itens=2",
        landing_file="{legislatura}_deputados_legislatura.json",
        options={"legislaturas": [56]},
    )


def test_extract_legislaturas_junta_as_paginas(tmp_path, monkeypatch):
    url = "http://api/deputados?idLegislatura=56&itens=2"
    last = {"rel": "last", "href": f"{url}&pagina=2"}
    http = PagedHttp(
        {
            f"{url}&pagina=1": {"dados": [{"id": 1}, {"id": 2}], "links": [last]},
            f"{url}&pagina=2": {"dados": [{"id": 3}], "links": [last]},
        }
    )
    monkeypatch.setattr(camara_etl, "HttpClient", lambda *a, **k: http)
    camara_etl.extract_legislaturas(_leg_cfg(tmp_path))
    data = http.saved["56_deputados_legislatura.json"]
    assert [d["id"] for d in data["dados"]] == [1, 2, 3]
    assert data["links"] == [{"rel": "self", "href": url}]


def test_extract_legislaturas_pula_se_uma_pagina_falha(tmp_path, monkeypatch):
    url = "http://api/deputados?idLegislatura=56&itens=2"
    last = {"rel": "last", "href": f"{url}&pagina=2"}
    http = PagedHttp({f"{url}&pagina=1": {"dados": [{"id": 1}], "links": [last]}})
    monkeypatch.setattr(camara_etl, "HttpClient", lambda *a, **k: http)
    camara_etl.extract_legislaturas(_leg_cfg(tmp_path))
    assert http.saved == {}


def _dep_cfg(tmp_path, historico: str | None) -> PipelineConfig:
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        parameter_dir=tmp_path / "params",
        landing_file="{id}_deputado.json",
        parameter_file="id_deputados.csv",
        options={"historico_file": "hist.csv"} if historico else {},
    )
    cfg.landing_dir.mkdir(parents=True, exist_ok=True)
    cfg.parameter_dir.mkdir(parents=True, exist_ok=True)
    cfg.parameter_filepath.write_text("id\n1\n2\n")
    if historico:
        (cfg.parameter_dir / "hist.csv").write_text(historico)
    return cfg


def test_deputados_a_baixar_atuais_sempre_e_historico_so_o_que_falta(tmp_path):
    cfg = _dep_cfg(tmp_path, "id\n1\n3\n4\n")
    (cfg.landing_dir / "1_deputado.json").write_text("{}")
    (cfg.landing_dir / "3_deputado.json").write_text("{}")
    assert camara_etl.deputados_a_baixar(cfg) == ["1", "2", "4"]


def test_deputados_a_baixar_sem_historico_so_atuais(tmp_path):
    assert camara_etl.deputados_a_baixar(_dep_cfg(tmp_path, None)) == ["1", "2"]


def test_transform_arquivo_anual_cabecalho_do_ano_mais_recente(tmp_path):
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
        bronze_file="b.csv",
    )
    cfg.landing_dir.mkdir(exist_ok=True)
    _write(cfg.landing_dir / "p-2001.json", {"dados": [{"id": 1, "s": {"a": "x"}}]})
    _write(
        cfg.landing_dir / "p-2024.json",
        {"dados": [{"id": 2, "s": {"a": "y", "novo": "z"}}]},
    )
    _write(cfg.landing_dir / "p-2002.json", {"dados": []})
    camara_etl.transform_arquivo_anual(cfg)
    df = pd.read_csv(cfg.bronze_filepath, sep=";", dtype=str)
    assert list(df.columns) == ["id", "s_a", "s_novo"]
    assert df["id"].tolist() == ["2", "1"]


def test_transform_votacoes_remove_votacao_repetida_entre_trimestres(tmp_path):
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
        bronze_file="b.csv",
    )
    v = {"id": "1-2", "uriProposicaoObjeto": "http://x/proposicoes/9"}
    _write(cfg.landing_dir / "votacoes_2025_Q3_1.json", {"dados": [v]})
    _write(cfg.landing_dir / "votacoes_2025_Q4_9.json", {"dados": [v, {"id": "3-4"}]})
    camara_etl.transform_votacoes(cfg)
    df = pd.read_csv(cfg.bronze_filepath, sep=";", dtype=str)
    assert df["id"].tolist() == ["1-2", "3-4"]
    assert df["id_proposicao"].tolist()[0] == "9"
