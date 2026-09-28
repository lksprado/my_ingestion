import json
from datetime import date, datetime
from pathlib import Path

import pytest

from core.config import PipelineConfig
from core.incremental import (
    extract_by_ids,
    landing_ids,
    last_complete_date,
    mark_no_data,
    missing_dates_from_landing,
    pending_ids,
    read_dates_csv,
    read_ids,
    write_dates_csv,
)


def test_last_complete_date_respects_cutoff():
    assert last_complete_date(now=datetime(2026, 9, 13, 10, 0)) == date(2026, 9, 12)
    assert last_complete_date(now=datetime(2026, 9, 13, 20, 0)) == date(2026, 9, 13)


def _dates_cfg(tmp_path, dias, **options) -> PipelineConfig:
    cfg = PipelineConfig(
        landing_dir=tmp_path / "landing",
        landing_file="day_summary_{day}.json",
        options={"control_file": "missing_dates.csv", **options},
    )
    for dia in dias:
        (cfg.landing_dir / f"day_summary_{dia}.json").write_text("{}")
    (cfg.landing_dir / "outro_2026-09-01.json").write_text("{}")  # fora do padrão
    return cfg


NOW = datetime(2026, 9, 13, 10, 0)  # último dia completo: 12/09


def test_missing_dates_fills_holes_and_the_tail(tmp_path):
    cfg = _dates_cfg(tmp_path, ["2026-09-05", "2026-09-06", "2026-09-08"])
    got = missing_dates_from_landing(cfg, now=NOW)
    # 07 é buraco no meio (o MAX(data) pularia); 09..12 é a cauda.
    assert got == ["2026-09-07", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-12"]
    assert read_dates_csv(cfg.landing_dir / "missing_dates.csv") == got


def test_missing_dates_empty_file_counts_as_missing(tmp_path):
    cfg = _dates_cfg(tmp_path, ["2026-09-10", "2026-09-11", "2026-09-12"])
    (cfg.landing_dir / "day_summary_2026-09-11.json").write_text("")
    assert missing_dates_from_landing(cfg, now=NOW) == ["2026-09-11"]


def test_missing_dates_old_holes_outside_lookback_are_left(tmp_path):
    cfg = _dates_cfg(
        tmp_path,
        ["2026-08-01", "2026-08-03", "2026-09-11", "2026-09-12"],
        lookback_days=5,
    )
    assert missing_dates_from_landing(cfg, now=NOW) == [
        "2026-09-08",
        "2026-09-09",
        "2026-09-10",
    ]


def test_missing_dates_long_stop_is_filled_entirely(tmp_path):
    cfg = _dates_cfg(tmp_path, ["2026-08-01"], lookback_days=3)
    got = missing_dates_from_landing(cfg, now=NOW)
    assert got[0] == "2026-08-02" and got[-1] == "2026-09-12" and len(got) == 42


def test_missing_dates_empty_landing_uses_the_window(tmp_path):
    cfg = _dates_cfg(tmp_path, [], lookback_days=2)
    assert missing_dates_from_landing(cfg, now=NOW) == ["2026-09-11", "2026-09-12"]


def test_missing_dates_up_to_date_writes_empty_control(tmp_path):
    cfg = _dates_cfg(tmp_path, ["2026-09-11", "2026-09-12"])
    assert missing_dates_from_landing(cfg, now=NOW) == []
    assert read_dates_csv(cfg.landing_dir / "missing_dates.csv") == []


def test_write_and_read_roundtrip(tmp_path):
    path = tmp_path / "sub" / "control.csv"
    write_dates_csv(["2026-01-01", "2026-01-02"], path)
    assert read_dates_csv(path) == ["2026-01-01", "2026-01-02"]

    write_dates_csv([], path)
    assert read_dates_csv(path) == []
    assert read_dates_csv(tmp_path / "missing.csv") == []


def test_pending_ids_preserves_order_and_skips(tmp_path):
    no_data = tmp_path / "sem_dados.csv"
    mark_no_data(no_data, "3")
    mark_no_data(no_data, "4")
    assert no_data.read_text() == "id\n3\n4\n"

    result = pending_ids(
        [5, "1", "2", "1", "3", "4"], done_ids=["2"], no_data_path=no_data
    )
    assert result == ["5", "1"]


def test_pending_ids_without_no_data_file(tmp_path):
    assert pending_ids(["a", "b"], [], tmp_path / "nao_existe.csv") == ["a", "b"]
    assert pending_ids(["a", "b"], ["a"], None) == ["b"]


def test_read_ids_coerces_floats_and_dedups(tmp_path):
    csv = tmp_path / "ids.csv"
    csv.write_text("idprocesso\n123.0\n123.0\n\n456.0\n")
    assert read_ids(csv, "idprocesso") == ["123", "456"]


def test_landing_ids_strips_suffix(tmp_path):
    (tmp_path / "10_votos_deputados.json").write_text("{}")
    (tmp_path / "20_votos_deputados.json").write_text("{}")
    (tmp_path / "ignorar.csv").write_text("")
    assert landing_ids(tmp_path, "_votos_deputados") == {"10", "20"}


class FakeHttp:
    """Respostas por URL: dado, ``None`` (timeout, sem status) ou um status HTTP."""

    def __init__(self, responses: dict):
        self.responses = responses
        self.saved = []
        self.requested = []

    def get_json_status(self, url):
        self.requested.append(url)
        resp = self.responses[url]
        if resp is None:
            return None, None
        if isinstance(resp, int):
            return None, resp
        return resp, 200

    def save_json(self, data, output_dir, filename):
        self.saved.append(filename)
        (Path(output_dir) / filename).write_text(json.dumps(data))


def _ids_cfg(tmp_path, **options) -> PipelineConfig:
    return PipelineConfig(
        landing_dir=tmp_path / "landing",
        parameter_dir=tmp_path / "params",
        url_base="http://api/{id}/votos",
        landing_file="{id}_votos.json",
        parameter_file="ids.csv",
        options={"no_data_file": "sem_dados.csv", **options},
    )


def _sem_dados(cfg) -> list[str]:
    path = cfg.parameter_dir / "sem_dados.csv"
    return path.read_text().split()[1:] if path.exists() else []


def _erros(cfg) -> list[str]:
    path = cfg.parameter_dir / "sem_dados_erros.csv"
    return path.read_text().splitlines() if path.exists() else []


def test_extract_by_ids_so_resposta_definitiva_vai_para_sem_dados(tmp_path):
    cfg = _ids_cfg(tmp_path)
    cfg.parameter_filepath.write_text("id\n1\n2\n3\n4\n5\n")
    (cfg.landing_dir / "1_votos.json").write_text("{}")  # já baixado
    http = FakeHttp(
        {
            "http://api/2/votos": [{"x": 1}],
            "http://api/3/votos": [],  # sem dados
            "http://api/4/votos": 404,  # não existe
            "http://api/5/votos": None,  # timeout: transitório
        }
    )
    extract_by_ids(cfg, http=http, today=date(2026, 9, 1))

    assert http.saved == ["2_votos.json"]
    assert _sem_dados(cfg) == ["3", "4"]
    assert _erros(cfg) == ["id,desde", "5,2026-09-01"]

    # Segunda rodada: só o 5 volta; agora responde e sai da lista de erros.
    http2 = FakeHttp({"http://api/5/votos": [{"x": 5}]})
    extract_by_ids(cfg, http=http2, today=date(2026, 9, 2))
    assert http2.requested == ["http://api/5/votos"]
    assert _erros(cfg) == []


def test_extract_by_ids_desiste_depois_de_dias_falhando(tmp_path):
    cfg = _ids_cfg(tmp_path, dias_para_desistir=7)
    cfg.parameter_filepath.write_text("id\n4\n5\n")
    responses = {"http://api/4/votos": 503, "http://api/5/votos": [{"x": 5}]}

    extract_by_ids(cfg, http=FakeHttp(responses), today=date(2026, 9, 1))
    # Mesma semana (retry do Airflow, execução seguinte): segue pendente.
    extract_by_ids(cfg, http=FakeHttp(responses), today=date(2026, 9, 7))
    assert _sem_dados(cfg) == []
    assert _erros(cfg) == ["id,desde", "4,2026-09-01"]

    extract_by_ids(cfg, http=FakeHttp(responses), today=date(2026, 9, 8))
    assert _sem_dados(cfg) == ["4"]
    assert _erros(cfg) == []


def test_extract_by_ids_nenhum_sucesso_falha_a_etapa(tmp_path):
    cfg = _ids_cfg(tmp_path)
    cfg.parameter_filepath.write_text("id\n4\n5\n")
    http = FakeHttp({"http://api/4/votos": None, "http://api/5/votos": 500})
    with pytest.raises(RuntimeError, match="nenhum sucesso"):
        extract_by_ids(cfg, http=http, today=date(2026, 9, 1))
    # Mesmo falhando, os erros ficam anotados para contar os dias.
    assert _erros(cfg) == ["id,desde", "4,2026-09-01", "5,2026-09-01"]
    assert _sem_dados(cfg) == []

    # Retry no mesmo dia (Airflow): a API segue fora, segue vermelho.
    with pytest.raises(RuntimeError, match="nenhum sucesso"):
        extract_by_ids(cfg, http=http, today=date(2026, 9, 1))
    # Dias depois, só IDs que já vinham falhando: aviso, não vermelho.
    extract_by_ids(cfg, http=http, today=date(2026, 9, 3))
    assert _erros(cfg) == ["id,desde", "4,2026-09-01", "5,2026-09-01"]


def test_extract_by_ids_custom_has_data(tmp_path):
    cfg = _ids_cfg(tmp_path)
    cfg.parameter_filepath.write_text("id\n7\n")
    http = FakeHttp({"http://api/7/votos": {"dados": []}})
    extract_by_ids(cfg, has_data=lambda d: bool(d.get("dados")), http=http)
    assert http.saved == []
    assert _sem_dados(cfg) == ["7"]


class RaisingHttp(FakeHttp):
    def get_json_status(self, url):
        if url == "http://api/5/votos":
            raise RuntimeError("boom")
        return super().get_json_status(url)


def test_extract_by_ids_with_workers_matches_sequential(tmp_path):
    cfg = _ids_cfg(tmp_path, workers=4)
    ids = list(range(1, 41))
    cfg.parameter_filepath.write_text("id\n" + "\n".join(map(str, ids)) + "\n")
    # pares têm dado, ímpares vêm vazios
    http = FakeHttp(
        {f"http://api/{i}/votos": [{"x": i}] if i % 2 == 0 else [] for i in ids}
    )
    extract_by_ids(cfg, http=http)

    assert sorted(http.saved) == sorted(f"{i}_votos.json" for i in ids if i % 2 == 0)
    assert sorted(_sem_dados(cfg), key=int) == [str(i) for i in ids if i % 2]


def test_extract_by_ids_thread_exception_does_not_blacklist(tmp_path):
    cfg = _ids_cfg(tmp_path, workers=2)
    cfg.parameter_filepath.write_text("id\n5\n6\n")
    http = RaisingHttp({"http://api/6/votos": [{"x": 1}]})
    extract_by_ids(cfg, http=http)
    assert http.saved == ["6_votos.json"]
    assert _sem_dados(cfg) == []
    assert _erros(cfg) == []  # bug nosso não conta para desistir do ID
