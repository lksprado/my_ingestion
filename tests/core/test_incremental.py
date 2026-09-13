from datetime import date, datetime

import pandas as pd
import pytest

from core.incremental import (
    mark_no_data,
    max_date,
    missing_dates,
    missing_dates_from_db,
    pending_ids,
    read_dates_csv,
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
