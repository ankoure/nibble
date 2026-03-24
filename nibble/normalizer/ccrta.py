"""Normalizer for CCRTA (Cape Cod Regional Transit Authority) feeds."""

from __future__ import annotations

import logging

from google.transit import gtfs_realtime_pb2

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer

logger = logging.getLogger(__name__)


class CcrtaNormalizer(BaseNormalizer):
    """Normalizer for CCRTA feeds.

    CCRTA's static GTFS uses internal numeric route_ids (e.g. ``"2976"``) with
    descriptive long names (e.g. ``"Sealine Hyannis-Falmouth/Woods Hole"``), but
    the RT feed reports abbreviated route names (e.g. ``"Sealine"``).  This
    normalizer remaps abbreviated names to static GTFS route_ids using
    case-insensitive prefix and substring matching against ``route_long_name``.
    """

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        """Remap CCRTA abbreviated route names to their static GTFS route_ids.

        Args:
            feed: The raw ``FeedMessage`` from the CCRTA feed.
            gtfs: Static GTFS indexes containing the ``route_short_names`` map
                (which for CCRTA holds long name → route_id entries).

        Returns:
            The modified ``FeedMessage`` with normalised route IDs.
        """
        for entity in feed.entity:
            route_id = entity.vehicle.trip.route_id
            if not route_id or route_id in gtfs.route_trips:
                continue

            # Exact match first (e.g. "Hyannis Crosstown").
            uuid = gtfs.route_short_names.get(route_id)
            if not uuid:
                # Case-insensitive prefix then substring match to handle
                # abbreviated names like "Sealine" → "Sealine Hyannis-Falmouth/Woods Hole"
                # or "Villager" → "Barnstable Villager".
                lower = route_id.lower()
                for name, rid in gtfs.route_short_names.items():
                    name_lower = name.lower()
                    if name_lower == lower or name_lower.startswith(lower) or lower in name_lower:
                        uuid = rid
                        break

            if uuid:
                logger.info("CCRTA: remapped route_id %r -> %r", route_id, uuid)
                entity.vehicle.trip.route_id = uuid
            else:
                logger.warning(
                    "CCRTA: unknown route_id %r - not in routes.txt (known names: %s)",
                    route_id,
                    ", ".join(sorted(gtfs.route_short_names)[:10]),
                )
                from nibble import unknown_routes

                unknown_routes.record(route_id)
        return feed
