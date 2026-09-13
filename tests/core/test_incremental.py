import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from core.config import PipelineConfig
from core.incremental import (
    extract_by_ids,
    landing_ids,
    mark_no_data,
    max_date,
    missing_dates,
    missing_dates_from_db,
    pending_ids,
    read_dates_csv,
    read_ids,
    write_dates_csv,
)


class FakeDb:
    def __init__(self, results: dict[str, object]):
        self.results = results

    def read_sql(self, sql):
        return pd.DataFrame({"max": [self.results[sql]]})


def test_no_gap_returns_empty():
    now = datetime(2026, 9, 13, 10, 0)
    assert missing_dates(date(2026, 9, 12), now=now) == []


def test_gap_until_yesterday_before_cutoff():
    now = datetime(2026, 9, 13, 10, 0)
    assert missing_dates(date(2026, 9, 9), now=now) == [
        "2026-09-10",
        "2026-09-11",
        "2026-09-12",
    ]


def test_includes_today_after_cutoff():
    now = datetime(2026, 9, 13, 20, 0)
    assert missing_dates(date(2026, 9, 12), now=now) == ["2026-09-13"]


def test_write_and_read_roundtrip(tmp_path):
    path = tmp_path / "sub" / "control.csv"
    write_dates_csv(["2026-01-01", "2026-01-02"], path)
    assert read_dates_csv(path) == ["2026-01-01", "2026-01-02"]

    write_dates_csv([], path)
    assert read_dates_csv(path) == []
    assert read_dates_csv(tmp_path / "missing.csv") == []


@pytest.mark.parametrize(
    "value", [date(2026, 1, 2), datetime(2026, 1, 2, 5), "2026-01-02 00:00:00"]
)
def test_max_date_accepts_date_datetime_and_str(value):
    assert max_date(FakeDb({"q": value}), "q") == date(2026, 1, 2)


def test_max_date_none_when_empty():
    assert max_date(FakeDb({"q": None}), "q") is None


def test_missing_dates_from_db_uses_min_mark_and_writes_control(tmp_path, monkeypatch):
    import core.incremental as mod

    monkeypatch.setattr(
        mod, "missing_dates", lambda since, cutoff_hour: [f"since={since}"]
    )
    db = FakeDb({"a": date(2026, 1, 5), "b": date(2026, 1, 3)})
    control = tmp_path / "ctl.csv"
    assert missing_dates_from_db(db, ["a", "b"], control) == ["since=2026-01-03"]
    assert read_dates_csv(control) == ["since=2026-01-03"]


def test_missing_dates_from_db_raises_on_empty_table(tmp_path):
    db = FakeDb({"a": date(2026, 1, 5), "b": None})
    with pytest.raises(ValueError, match="High-water mark"):
        missing_dates_from_db(db, ["a", "b"], tmp_path / "ctl.csv")


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
    def __init__(self, responses: dict):
        self.responses = responses
        self.saved = []

    def get_json(self, url):
        return self.responses[url]

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


def test_extract_by_ids_skips_done_and_blacklists(tmp_path):
    cfg = _ids_cfg(tmp_path)
    cfg.parameter_filepath.write_text("id\n1\n2\n3\n4\n")
    (cfg.landing_dir / "1_votos.json").write_text("{}")  # já baixado
    http = FakeHttp(
        {
            "http://api/2/votos": [{"x": 1}],
            "http://api/3/votos": [],  # sem dados
            "http://api/4/votos": None,  # timeout
        }
    )
    extract_by_ids(cfg, http=http)

    assert http.saved == ["2_votos.json"]
    no_data = (cfg.parameter_dir / "sem_dados.csv").read_text().split()
    assert no_data == ["id", "3", "4"]

    # Segunda rodada: nada pendente, nenhuma requisição.
    http2 = FakeHttp({})
    extract_by_ids(cfg, http=http2)
    assert http2.saved == []


def test_extract_by_ids_without_blacklist_on_error(tmp_path):
    cfg = _ids_cfg(tmp_path, blacklist_on_error=False)
    cfg.parameter_filepath.write_text("id\n4\n")
    extract_by_ids(cfg, http=FakeHttp({"http://api/4/votos": None}))
    assert not (cfg.parameter_dir / "sem_dados.csv").exists()


def test_extract_by_ids_custom_has_data(tmp_path):
    cfg = _ids_cfg(tmp_path)
    cfg.parameter_filepath.write_text("id\n7\n")
    http = FakeHttp({"http://api/7/votos": {"dados": []}})
    extract_by_ids(cfg, has_data=lambda d: bool(d.get("dados")), http=http)
    assert http.saved == []
