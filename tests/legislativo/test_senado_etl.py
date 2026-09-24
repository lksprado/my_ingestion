import json
from datetime import date

import pandas as pd

from core import PipelineConfig
from pipelines.legislativo.senado import senado_etl


def test_anos_vai_do_inicio_ao_ano_corrente():
    cfg = PipelineConfig(landing_dir="x", options={"ano_inicio": 2018})
    got = senado_etl.anos(cfg)
    assert got[0] == 2018 and got[-1] == date.today().year


def test_anos_sem_opcao_usa_default():
    assert senado_etl.anos(PipelineConfig(landing_dir="x"))[0] == 2001


def test_status_encerrado(tmp_path):
    fora = tmp_path / "fora.json"
    fora.write_text(json.dumps([{"tramitando": "Não"}]))
    tramita = tmp_path / "tramita.json"
    tramita.write_text(json.dumps([{"tramitando": "Sim"}]))
    vazio = tmp_path / "vazio.json"
    vazio.write_text("[]")
    assert senado_etl._status_encerrado(fora)
    assert not senado_etl._status_encerrado(tramita)
    assert not senado_etl._status_encerrado(vazio)
    assert not senado_etl._status_encerrado(tmp_path / "nao_existe.json")


class FlakyHttp:
    """Falha (``None``) nas primeiras ``falhas`` chamadas de cada URL."""

    def __init__(self, data, falhas):
        self.data, self.falhas, self.calls, self.saved = data, falhas, {}, []

    def get_json(self, url):
        self.calls[url] = self.calls.get(url, 0) + 1
        return None if self.calls[url] <= self.falhas else self.data

    def save_json(self, data, output_dir, filename):
        self.saved.append(filename)


def _processos_cfg(tmp_path) -> PipelineConfig:
    ano = date.today().year
    return PipelineConfig(
        landing_dir=tmp_path,
        url_base="http://api/processo?ano={ano}",
        landing_file="{ano}_processos.json",
        options={"ano_inicio": ano, "tentativas": 3},
    )


def test_extract_processos_tenta_de_novo_resposta_cortada(tmp_path, monkeypatch):
    http = FlakyHttp([{"id": 1}], falhas=2)
    monkeypatch.setattr(senado_etl, "HttpClient", lambda *a, **k: http)
    senado_etl.extract_processos(_processos_cfg(tmp_path))
    assert http.saved == [f"{date.today().year}_processos.json"]


def test_extract_processos_desiste_depois_das_tentativas(tmp_path, monkeypatch):
    http = FlakyHttp([{"id": 1}], falhas=3)
    monkeypatch.setattr(senado_etl, "HttpClient", lambda *a, **k: http)
    senado_etl.extract_processos(_processos_cfg(tmp_path))
    assert http.saved == [] and list(http.calls.values()) == [3]


def test_transform_processos(tmp_path):
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
        bronze_file="b.csv",
    )
    cfg.landing_dir.mkdir(exist_ok=True)
    (cfg.landing_dir / "2023_processos.json").write_text(json.dumps([{"id": 1}]))
    (cfg.landing_dir / "2024_processos.json").write_text(
        json.dumps([{"id": 2, "tramitando": "Sim"}])
    )
    senado_etl.transform_processos(cfg)
    df = pd.read_csv(cfg.bronze_filepath, sep=";", dtype=str)
    assert list(df.columns) == ["id", "tramitando"]
    assert df["id"].tolist() == ["2", "1"]
