"""Parse feed_info.txt from a GTFS ZIP to extract feed metadata."""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass


@dataclass
class FeedInfo:
    """Metadata from a GTFS feed_info.txt file.

    Attributes:
        feed_start_date: Date the feed becomes valid, in ``YYYYMMDD`` format.
        feed_end_date: Date after which the feed is no longer valid, in ``YYYYMMDD`` format.
        feed_version: Publisher-assigned version string for this feed.
    """

    feed_start_date: str  # YYYYMMDD
    feed_end_date: str  # YYYYMMDD
    feed_version: str


def dates_from_calendar(zf: zipfile.ZipFile) -> tuple[str, str]:
    """Derive feed start/end dates from calendar.txt and calendar_dates.txt.

    Returns a ``(start_date, end_date)`` tuple in ``YYYYMMDD`` format.  Falls
    back to empty strings when neither file is present.
    """
    names = set(zf.namelist())
    min_date = ""
    max_date = ""

    def _track(val: str) -> None:
        nonlocal min_date, max_date
        if val:
            if not min_date or val < min_date:
                min_date = val
            if val > max_date:
                max_date = val

    if "calendar.txt" in names:
        with zf.open("calendar.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                for field in ("start_date", "end_date"):
                    _track(row.get(field, "").strip())

    if "calendar_dates.txt" in names:
        with zf.open("calendar_dates.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                _track(row.get("date", "").strip())

    return min_date, max_date


def parse_feed_info(zip_content: bytes) -> FeedInfo | None:
    """Extract FeedInfo from a GTFS ZIP's feed_info.txt.

    Args:
        zip_content: Raw bytes of a GTFS ZIP archive.

    Returns:
        A ``FeedInfo`` populated from the first row of ``feed_info.txt``, or
        ``None`` if the file is absent from the archive.
    """
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        if "feed_info.txt" not in zf.namelist():
            return None
        with zf.open("feed_info.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                return FeedInfo(
                    feed_start_date=row.get("feed_start_date", "").strip(),
                    feed_end_date=row.get("feed_end_date", "").strip(),
                    feed_version=row.get("feed_version", "").strip(),
                )
    return None
