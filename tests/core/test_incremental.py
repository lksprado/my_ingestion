from datetime import date, datetime

from core.incremental import missing_dates, read_dates_csv, write_dates_csv


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
