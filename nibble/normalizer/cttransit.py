"""Normalizer for CTTransit (Connecticut Transit) feeds."""

from __future__ import annotations

import logging

from google.transit import gtfs_realtime_pb2

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer

logger = logging.getLogger(__name__)


class CttransitNormalizer(BaseNormalizer):
    """Normalizer for CTTransit GTFS-RT feeds.

    CTTransit's feed populates ``trip_id`` but omits ``route_id``. This
    normalizer fills in ``route_id`` from the static GTFS ``trips`` index.
    """

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vehicle = entity.vehicle
            if vehicle.trip.route_id:
                continue
            trip_id = vehicle.trip.trip_id
            if not trip_id:
                continue
            trip = gtfs.trips.get(trip_id)
            if trip is None:
                logger.debug("CTTransit: trip_id %r not found in static GTFS", trip_id)
                continue
            logger.debug("CTTransit: filled route_id %r for trip_id %r", trip.route_id, trip_id)
            vehicle.trip.route_id = trip.route_id
        return feed
