"""Normalizer for VTA (Vineyard Transit Authority) feeds."""

from __future__ import annotations

import logging

from google.transit import gtfs_realtime_pb2

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer

logger = logging.getLogger(__name__)


class VtaNormalizer(BaseNormalizer):
    """Normalizer for VTA (Vineyard Transit Authority) feeds.

    The MyTransitRide API reports ``headsignText`` values (e.g. ``"3"``,
    ``"10"``) as route identifiers, which match GTFS ``route_short_name`` but
    not the internal numeric ``route_id`` (e.g. ``"2801"``, ``"2808"``). This
    normalizer remaps them using the ``route_short_names`` index built from
    ``routes.txt``.
    """

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        for entity in feed.entity:
            route_id = entity.vehicle.trip.route_id
            if not route_id or route_id in gtfs.route_trips:
                continue
            uuid = gtfs.route_short_names.get(route_id)
            if uuid:
                logger.info("VTA: remapped route_id %r -> %r", route_id, uuid)
                entity.vehicle.trip.route_id = uuid
            else:
                logger.warning(
                    "VTA: unknown route_id %r - not in routes.txt (known short names: %s)",
                    route_id,
                    ", ".join(sorted(gtfs.route_short_names)[:10]),
                )
                from nibble import unknown_routes

                unknown_routes.record(route_id)
        return feed
