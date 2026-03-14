"""Fix common issues in GTFS static ZIP archives before loading or publishing.

Each fixer is a callable that receives the raw text of a single CSV file and
returns corrected text. Fixers are applied to every .txt file in the ZIP.
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Callable

FileFixer = Callable[[str], str]


def _strip_field_whitespace(text: str) -> str:
    """Strip leading/trailing tabs and spaces from every CSV field value.

    Fixes agencies like Metra that prefix field values with a tab character,
    causing trip_id lookups against static GTFS to fail silently.
    """
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    for line in lines:
        stripped = ",".join(field.strip() for field in line.rstrip("\r\n").split(","))
        ending = line[len(line.rstrip("\r\n")) :]
        out.append(stripped + ending)
    return "".join(out)


def _normalize_line_endings(text: str) -> str:
    """Normalise CRLF → LF."""
    return text.replace("\r\n", "\n")


def _strip_utf8_bom(text: str) -> str:
    """Remove UTF-8 BOM if present at the start of the file."""
    return text.lstrip("\ufeff")


_FIXERS: list[FileFixer] = [
    _strip_utf8_bom,
    _normalize_line_endings,
    _strip_field_whitespace,
]


def _synthesize_calendar(calendar_dates_text: str) -> str:
    """Generate a synthetic ``calendar.txt`` from ``calendar_dates.txt``.

    For feeds that omit ``calendar.txt`` and express their schedule entirely
    via ``calendar_dates.txt``, this function derives the operating days of
    week for each service from the dates with ``exception_type=1`` (service
    added) and produces a minimal ``calendar.txt`` with one row per service.

    Args:
        calendar_dates_text: UTF-8 text content of ``calendar_dates.txt``.

    Returns:
        UTF-8 text content of a synthetic ``calendar.txt``.
    """
    # service_id -> set of weekday ints (0=Mon … 6=Sun), min/max date strings
    service_dates: dict[str, list[str]] = defaultdict(list)
    reader = csv.DictReader(io.StringIO(calendar_dates_text))
    for row in reader:
        if row.get("exception_type", "").strip() == "1":
            service_dates[row["service_id"].strip()].append(row["date"].strip())

    day_cols = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=["service_id"] + day_cols + ["start_date", "end_date"])
    writer.writeheader()
    for service_id, dates in service_dates.items():
        weekdays: set[int] = set()
        for d in dates:
            weekdays.add(datetime.strptime(d, "%Y%m%d").weekday())
        row_out: dict[str, str] = {"service_id": service_id}
        for i, col in enumerate(day_cols):
            row_out[col] = "1" if i in weekdays else "0"
        row_out["start_date"] = min(dates)
        row_out["end_date"] = max(dates)
        writer.writerow(row_out)
    return out.getvalue()


def fix_gtfs_zip(content: bytes) -> bytes:
    """Apply all registered fixers to every .txt file in a GTFS ZIP.

    Non-CSV files are passed through unchanged. The returned ZIP has the same
    file structure as the input but with corrected CSV content.

    Fixers applied (in order): BOM stripping, CRLF normalisation, field
    whitespace stripping.

    Args:
        content: Raw bytes of the original GTFS ZIP archive.

    Returns:
        Bytes of a new ZIP archive with all ``.txt`` files corrected.
    """
    buf = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(content)) as src,
        zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as dst,
    ):
        names = src.namelist()
        calendar_dates_text: str | None = None
        for name in names:
            raw = src.read(name)
            if name.endswith(".txt"):
                text = raw.decode("utf-8-sig")
                for fixer in _FIXERS:
                    text = fixer(text)
                if name == "calendar_dates.txt":
                    calendar_dates_text = text
                dst.writestr(name, text.encode("utf-8"))
            else:
                dst.writestr(name, raw)
        if "calendar.txt" not in names and calendar_dates_text is not None:
            dst.writestr("calendar.txt", _synthesize_calendar(calendar_dates_text).encode("utf-8"))
    return buf.getvalue()
