"""Tests for nibble.gtfs.feed_info - date derivation from calendar files."""

from __future__ import annotations

import io
import zipfile

from nibble.gtfs.feed_info import dates_from_calendar


def _make_zip(**files: str) -> zipfile.ZipFile:
    """Return an in-memory ZipFile containing the given filename→csv-text entries."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    buf.seek(0)
    return zipfile.ZipFile(buf)


def test_dates_from_calendar_txt() -> None:
    content = (
        "service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday\n"
    )
    content += "WD,20260101,20260630,1,1,1,1,1,0,0\n"
    content += "WE,20260103,20260628,0,0,0,0,0,1,1\n"
    with _make_zip(**{"calendar.txt": content}) as zf:
        start, end = dates_from_calendar(zf)
    assert start == "20260101"
    assert end == "20260630"


def test_dates_from_calendar_dates_txt() -> None:
    content = "service_id,date,exception_type\n"
    content += "WD,20260301,1\n"
    content += "WD,20260615,1\n"
    content += "WD,20260101,1\n"
    with _make_zip(**{"calendar_dates.txt": content}) as zf:
        start, end = dates_from_calendar(zf)
    assert start == "20260101"
    assert end == "20260615"


def test_dates_from_both_files() -> None:
    cal = (
        "service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday\n"
    )
    cal += "WD,20260201,20260501,1,1,1,1,1,0,0\n"
    cal_dates = "service_id,date,exception_type\n"
    cal_dates += "HOLIDAY,20260704,1\n"  # extends end beyond calendar.txt
    with _make_zip(**{"calendar.txt": cal, "calendar_dates.txt": cal_dates}) as zf:
        start, end = dates_from_calendar(zf)
    assert start == "20260201"
    assert end == "20260704"


def test_dates_empty_zip() -> None:
    with _make_zip() as zf:
        start, end = dates_from_calendar(zf)
    assert start == ""
    assert end == ""
