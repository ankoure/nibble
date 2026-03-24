"""Normalizer for RIPTA (Rhode Island Public Transit Authority) feeds."""

from __future__ import annotations

import logging

from nibble.protos import gtfs_realtime_pb2

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer

logger = logging.getLogger(__name__)


class RiptaNormalizer(BaseNormalizer):
    """Normalizer for RIPTA (Rhode Island Public Transit Authority) feeds.

    RIPTA feeds may include trip_ids that don't directly match trips.txt.
    This normalizer attempts to reconcile known formatting quirks.
    """

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        """Strip date suffixes from RIPTA trip_ids that don't match static GTFS.

        RIPTA sometimes appends a date component to trip IDs (e.g.
        ``"trip-123_20240101"``). This method strips the suffix and remaps the
        entity to the base trip ID when the base is found in static GTFS.

        Args:
            feed: The raw ``FeedMessage`` from the RIPTA feed.
            gtfs: Static GTFS indexes used to check whether a trip ID is valid.

        Returns:
            The modified ``FeedMessage`` with normalised trip IDs.
        """
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vehicle = entity.vehicle
            if not vehicle.HasField("trip"):
                continue
            trip_id = vehicle.trip.trip_id
            if not trip_id or trip_id in gtfs.trips:
                continue
            # RIPTA sometimes suffixes trip_ids with a date component like "_20240101".
            # Try stripping the suffix to find a match.
            normalized = trip_id.split("_")[0]
            if normalized in gtfs.trips:
                logger.debug("RIPTA: remapped trip_id %r -> %r", trip_id, normalized)
                vehicle.trip.trip_id = normalized
        return feed
