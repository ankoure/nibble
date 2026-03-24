"""Normalizer for Passio GO! feeds.

After the PassioAdapter maps ``routeId`` → ``shortName`` (or ``name``) via the
routes endpoint, this normalizer resolves any values that don't directly match
a GTFS ``route_id`` by looking them up in the ``route_short_names`` index
(short_name → route_id).  Values that match neither are logged and recorded
in ``unknown_routes``.
"""

from __future__ import annotations

import logging

from nibble.protos import gtfs_realtime_pb2

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer

logger = logging.getLogger(__name__)


class PassioNormalizer(BaseNormalizer):
    """Resolve Passio route short names / display names to GTFS route_ids."""

    # Directional suffixes Passio appends that don't appear in GTFS route names
    _DIRECTION_SUFFIXES = (" South", " North", " East", " West", " Loop")

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        for entity in feed.entity:
            route_id = entity.vehicle.trip.route_id
            if not route_id or route_id in gtfs.route_trips:
                continue

            mapped = self._resolve(route_id, gtfs)
            if mapped:
                logger.debug("Passio: remapped route_id %r -> %r", route_id, mapped)
                entity.vehicle.trip.route_id = mapped
            else:
                logger.warning("Passio: unknown route_id %r", route_id)
                from nibble import unknown_routes

                unknown_routes.record(route_id)
        return feed

    def _resolve(self, route_id: str, gtfs: StaticGTFS) -> str | None:
        # Direct match in route_short_names (covers both short name and long name)
        mapped = gtfs.route_short_names.get(route_id)
        if mapped:
            return mapped

        # "Route 4" → try short name "4"
        if route_id.lower().startswith("route "):
            mapped = gtfs.route_short_names.get(route_id[len("route ") :])
            if mapped:
                return mapped

        # "Gardner Route 1 South" → strip directional suffix → "Gardner Route 1"
        for suffix in self._DIRECTION_SUFFIXES:
            if route_id.endswith(suffix):
                mapped = gtfs.route_short_names.get(route_id[: -len(suffix)])
                if mapped:
                    return mapped

        return None
