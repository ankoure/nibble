"""Registry for route IDs seen in feeds that could not be matched to static GTFS.

Normalizers call :func:`record` whenever a route ID arrives from the upstream
API that has no corresponding entry in the static GTFS.  The counts are
exposed via the ``GET /unknown_routes`` endpoint so operators can spot missing
mappings without trawling log files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class UnknownRouteEntry:
    route_id: str
    count: int = 0
    first_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# Module-level registry.  Safe without locks because nibble runs in a single
# asyncio event loop - there are no concurrent writes.
_registry: dict[str, UnknownRouteEntry] = {}


def record(route_id: str) -> None:
    """Record an occurrence of an unmatched route ID.

    Args:
        route_id: The raw route identifier from the upstream feed.
    """
    now = datetime.now(timezone.utc)
    if route_id in _registry:
        _registry[route_id].count += 1
        _registry[route_id].last_seen = now
    else:
        _registry[route_id] = UnknownRouteEntry(route_id=route_id, count=1)


def all_entries() -> list[dict[str, object]]:
    """Return all recorded unknown route IDs, sorted by descending count.

    Returns:
        A list of dicts with ``route_id``, ``count``, ``first_seen``, and
        ``last_seen`` keys, ready for JSON serialisation.
    """
    return [
        {
            "route_id": e.route_id,
            "count": e.count,
            "first_seen": e.first_seen.isoformat(),
            "last_seen": e.last_seen.isoformat(),
        }
        for e in sorted(_registry.values(), key=lambda e: e.count, reverse=True)
    ]


def clear() -> None:
    """Remove all recorded entries (e.g. after deploying a normalizer fix)."""
    _registry.clear()
