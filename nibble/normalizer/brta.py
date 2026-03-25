"""Normalizer for BRTA (Berkshire Regional Transit Authority) feeds."""

from __future__ import annotations

import logging
import re

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer
from nibble.protos import gtfs_realtime_pb2

logger = logging.getLogger(__name__)

# Strips leading words like "Wk Rt ", "Rte ", "Route " from masterRouteId
_PREFIX_RE = re.compile(r"^(?:Wk\s+)?(?:Rte?\.?\s+|Route\s+)", re.IGNORECASE)

# Extracts the leading route number (digits + optional letter suffix, e.g. "5A")
_NUMBER_RE = re.compile(r"^(\d+[A-Za-z]?)")


def _candidate_short_name(route_id: str) -> str | None:
    """Extract a GTFS route_short_name candidate from a RouteMatch masterRouteId.

    Examples::

        "Wk Rt 01"      -> "1"
        "Rte 34"        -> "34"
        "Route 5 Loop"  -> "5"
        "Rte 21 Express"-> "21"

    Returns ``None`` if no numeric token can be extracted.
    """
    stripped = _PREFIX_RE.sub("", route_id).strip()
    m = _NUMBER_RE.match(stripped)
    if not m:
        return None
    token = m.group(1)
    # Separate digits from optional letter suffix, strip leading zeros
    digits = token.rstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
    suffix = token[len(digits) :].upper()
    try:
        return str(int(digits)) + suffix
    except ValueError:
        return None


class BrtaNormalizer(BaseNormalizer):
    """Normalizer for BRTA (Berkshire Regional Transit Authority) feeds.

    The RouteMatch API reports ``masterRouteId`` values like ``"Wk Rt 01"``
    and ``tripId`` values like ``"Rte 01 1130 in"`` that do not match BRTA's
    static GTFS. This normalizer:

    1. Remaps route_id by extracting the numeric short name and looking it up
       in the ``route_short_names`` index built from ``routes.txt``.
    2. Clears trip_id entirely, since RouteMatch trip IDs have no relationship
       to GTFS trip IDs. This lets nibble's position-based trip inference run
       rather than failing to find the trip silently.
    """

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        for entity in feed.entity:
            route_id = entity.vehicle.trip.route_id
            trip_id = entity.vehicle.trip.trip_id

            # Always clear the RouteMatch trip_id - it will never match GTFS
            if trip_id:
                entity.vehicle.trip.trip_id = ""

            if not route_id or route_id in gtfs.route_trips:
                continue

            candidate = _candidate_short_name(route_id)
            if candidate is None:
                logger.warning("BRTA: could not extract short name from route_id %r", route_id)
                continue

            gtfs_route_id = gtfs.route_short_names.get(candidate)
            if gtfs_route_id:
                logger.info("BRTA: remapped route_id %r -> %r", route_id, gtfs_route_id)
                entity.vehicle.trip.route_id = gtfs_route_id
            else:
                logger.warning(
                    "BRTA: no GTFS match for route_id %r (candidate %r; known short names: %s)",
                    route_id,
                    candidate,
                    ", ".join(sorted(gtfs.route_short_names)[:10]),
                )
                from nibble import unknown_routes

                unknown_routes.record(route_id)

        return feed
