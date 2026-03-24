"""Normalizer for VTA (Vineyard Transit Authority) feeds."""

from __future__ import annotations

import logging

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer
from nibble.protos import gtfs_realtime_pb2

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
        """Remap VTA ``headsignText`` route identifiers to internal GTFS route IDs.

        The MyTransitRide API sets ``trip.route_id`` to a ``route_short_name``
        value (e.g. ``"3"``, ``"10"``). This method replaces those values with
        the corresponding internal ``route_id`` from ``routes.txt``, logging a
        warning and recording any unmatched identifiers in ``unknown_routes``.

        Args:
            feed: The raw FeedMessage from the VTA adapter.
            gtfs: Loaded static GTFS providing the ``route_short_names`` index.

        Returns:
            The same FeedMessage with ``route_id`` fields remapped in place.
        """
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
