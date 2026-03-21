"""Tests for nibble.gtfs.fixer and nibble.gtfs.feed_info."""

from __future__ import annotations

import csv
import io
import zipfile

from nibble.gtfs.feed_info import parse_feed_info
from nibble.gtfs.fixer import fix_gtfs_zip

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_zip(files: dict[str, str]) -> bytes:
    """Build a GTFS ZIP in memory from a dict of filename → CSV text."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content.encode("utf-8"))
    return buf.getvalue()


def _read_csv_from_zip(zip_bytes: bytes, filename: str) -> list[dict[str, str]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open(filename) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            return list(reader)


# ---------------------------------------------------------------------------
# fixer tests
# ---------------------------------------------------------------------------


def test_fix_strips_tab_from_trip_id() -> None:
    """Metra-style tabs before trip_id values should be removed."""
    raw = _make_zip(
        {
            "trips.txt": "route_id,service_id,trip_id\nR1,S1,\tTRIP001\nR2,S2,\tTRIP002\n",
        }
    )
    fixed = fix_gtfs_zip(raw)
    rows = _read_csv_from_zip(fixed, "trips.txt")
    assert rows[0]["trip_id"] == "TRIP001"
    assert rows[1]["trip_id"] == "TRIP002"


def test_fix_strips_leading_spaces() -> None:
    raw = _make_zip(
        {
            "trips.txt": "route_id,service_id,trip_id\n R1, S1, TRIP001\n",
        }
    )
    fixed = fix_gtfs_zip(raw)
    rows = _read_csv_from_zip(fixed, "trips.txt")
    assert rows[0]["route_id"] == "R1"
    assert rows[0]["trip_id"] == "TRIP001"


def test_fix_normalizes_crlf() -> None:
    raw = _make_zip({"stop_times.txt": "trip_id,stop_id\r\nT1,S1\r\nT2,S2\r\n"})
    fixed = fix_gtfs_zip(raw)
    with zipfile.ZipFile(io.BytesIO(fixed)) as zf:
        content = zf.read("stop_times.txt").decode("utf-8")
    assert "\r\n" not in content
    assert "T1,S1" in content


def test_fix_strips_bom() -> None:
    bom = "\ufeff"
    raw = _make_zip({"trips.txt": f"{bom}route_id,trip_id\nR1,T1\n"})
    fixed = fix_gtfs_zip(raw)
    with zipfile.ZipFile(io.BytesIO(fixed)) as zf:
        content = zf.read("trips.txt").decode("utf-8")
    assert not content.startswith("\ufeff")


def test_fix_preserves_non_txt_files() -> None:
    binary_data = b"\x00\x01\x02\x03"
    raw = _make_zip({"trips.txt": "route_id,trip_id\nR1,T1\n"})
    # Add a binary file manually
    buf = io.BytesIO(raw)
    with zipfile.ZipFile(buf, mode="a") as zf:
        zf.writestr("extra.bin", binary_data)
    raw_with_bin = buf.getvalue()

    fixed = fix_gtfs_zip(raw_with_bin)
    with zipfile.ZipFile(io.BytesIO(fixed)) as zf:
        assert zf.read("extra.bin") == binary_data


def test_fix_passthrough_on_clean_feed() -> None:
    """A feed with no issues should come out identical (field-for-field)."""
    raw = _make_zip(
        {
            "trips.txt": "route_id,service_id,trip_id\nR1,S1,TRIP001\n",
            "stop_times.txt": "trip_id,stop_id,stop_sequence\nTRIP001,STOP1,1\n",
        }
    )
    fixed = fix_gtfs_zip(raw)
    trips = _read_csv_from_zip(fixed, "trips.txt")
    assert trips[0] == {"route_id": "R1", "service_id": "S1", "trip_id": "TRIP001"}


# ---------------------------------------------------------------------------
# feed_info tests
# ---------------------------------------------------------------------------


def test_parse_feed_info_present() -> None:
    raw = _make_zip(
        {
            "feed_info.txt": (
                "feed_publisher_name,feed_start_date,feed_end_date,feed_version\n"
                "TestAgency,20260101,20260331,Winter 2026\n"
            ),
        }
    )
    info = parse_feed_info(raw)
    assert info is not None
    assert info.feed_start_date == "20260101"
    assert info.feed_end_date == "20260331"
    assert info.feed_version == "Winter 2026"


def test_parse_feed_info_absent() -> None:
    raw = _make_zip({"trips.txt": "route_id,trip_id\nR1,T1\n"})
    assert parse_feed_info(raw) is None
