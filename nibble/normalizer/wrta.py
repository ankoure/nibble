"""Normalizer for WRTA (Worcester Regional Transit Authority) feeds.

WRTA uses a Swiv-based real-time feed.  When ``NIBBLE_SWIV_TOPO_URL`` is set,
the SwivAdapter already replaces ``idLigne`` integers with ``nomCommercial``
route names before this normalizer runs, so the feed arrives with human-readable
route IDs that match the GTFS directly.

This normalizer acts as a fallback for any ``idLigne`` values the topo endpoint
didn't cover, using a static map built from known Swiv line IDs.
"""

from __future__ import annotations

import logging

from nibble.protos import gtfs_realtime_pb2

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.swiv import SwivNormalizer

logger = logging.getLogger(__name__)

# Mapping from Swiv idLigne → GTFS route_id.
# To fill in gaps: cross-reference the Swiv vehicle feed's idLigne + destination
# against routes.txt route_long_name values, or request a line list from WRTA.
_LIGNE_TO_ROUTE: dict[str, str] = {
    "18045": "27",  # Auburn Mall via Main St.
    "18046": "2",  # Tatnuck Square via Pleasant St.
    "18047": "4",  # Shoppes at Blackstone Valley via Millbury St.
    "18062": "30",  # W. Boylston Wal-Mart via Grove St. & W. Boylston St.
    "18063": "33",  # Spencer–Brookfield via Main St. & Rt. 9 (through Leicester)
    "18068": "1",  # Mount St. Ann via Providence St. (Dorchester St corridor)
    "18069": "11",  # The Fair Plaza via Vernon Hill and Greenwood St.
    "18070": "7",  # Washington Heights Apts.
    # TODO: confirm the remaining idLigne values from the Swiv topo endpoint
    # "18054": "5",   # Southwest Commons via Grafton St. (or "12"?)
    # "18057": "?",   # Central Hub only — route unclear
    # "18060": "?",   # Lincoln Plaza — route 16 or 31?
    # "18061": "?",   # Central Hub only — route unclear
}


class WrtaNormalizer(SwivNormalizer):
    """Normalizer for WRTA: applies the hard-coded idLigne fallback map first,
    then delegates to SwivNormalizer for route_short_names lookup."""

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        for entity in feed.entity:
            route_id = entity.vehicle.trip.route_id
            if not route_id or route_id in gtfs.route_trips:
                continue
            mapped = _LIGNE_TO_ROUTE.get(route_id)
            if mapped:
                logger.debug("WRTA: remapped idLigne %r -> route_id %r", route_id, mapped)
                entity.vehicle.trip.route_id = mapped
        return super().normalize(feed, gtfs)
