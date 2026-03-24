"""Generic normalizer for Swiv-based feeds.

After the SwivAdapter maps ``idLigne`` → ``nomCommercial`` via the topo
endpoint, this normalizer resolves any ``nomCommercial`` values that don't
directly match a GTFS ``route_id`` by looking them up in the
``route_short_names`` index (short_name → route_id).  Values that match
neither are logged and recorded in ``unknown_routes``.
"""

from __future__ import annotations

import logging

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer
from nibble.protos import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


class SwivNormalizer(BaseNormalizer):
    """Resolve Swiv nomCommercial values to GTFS route_ids."""

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        for entity in feed.entity:
            route_id = entity.vehicle.trip.route_id
            if not route_id or route_id in gtfs.route_trips:
                continue
            mapped = gtfs.route_short_names.get(route_id)
            if mapped:
                logger.debug("Swiv: remapped route_id %r -> %r", route_id, mapped)
                entity.vehicle.trip.route_id = mapped
            else:
                logger.warning("Swiv: unknown route_id %r after topo mapping", route_id)
                from nibble import unknown_routes

                unknown_routes.record(route_id)
        return feed
