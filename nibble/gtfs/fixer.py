"""Fix common issues in GTFS static ZIP archives before loading or publishing.

Each fixer is a callable that receives the raw text of a single CSV file and
returns corrected text. Fixers are applied to every .txt file in the ZIP.
"""

from __future__ import annotations

import io
import zipfile
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
        for name in src.namelist():
            raw = src.read(name)
            if name.endswith(".txt"):
                text = raw.decode("utf-8-sig")
                for fixer in _FIXERS:
                    text = fixer(text)
                dst.writestr(name, text.encode("utf-8"))
            else:
                dst.writestr(name, raw)
    return buf.getvalue()
