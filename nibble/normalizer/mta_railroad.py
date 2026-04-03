"""Normalizer for MTA Railroad feeds (Metro-North and LIRR)."""

from __future__ import annotations

import logging

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer
from nibble.protos import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


class MtaRailroadNormalizer(BaseNormalizer):
    """Normalizer for MTA Railroad GTFS-RT feeds (Metro-North and LIRR).

    Both MNR and LIRR feeds report train numbers as trip IDs (e.g. ``"1300"``)
    while their static GTFS files use composite or opaque trip IDs. The train
    number corresponds to the ``trip_short_name`` field in ``trips.txt``.

    This normalizer rewrites each RT train-number trip ID to its full static
    trip ID via the ``trip_short_names`` index built by the static GTFS loader,
    so that downstream stop-time and interpolation lookups succeed.
    """

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        """Rewrite MTA Railroad train numbers to their full static GTFS trip IDs.

        Args:
            feed: The raw ``FeedMessage`` from the adapter.
            gtfs: Static GTFS indexes, including the ``trip_short_names``
                mapping populated from ``trips.txt``.

        Returns:
            The modified ``FeedMessage`` with train-number trip IDs replaced
            by their corresponding static trip IDs where a match exists.
            Unmatched IDs are left unchanged.
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
            full_id = gtfs.trip_short_names.get(trip_id)
            if full_id:
                logger.debug("MTA Railroad: rewrote trip_id %r -> %r", trip_id, full_id)
                vehicle.trip.trip_id = full_id

        return feed
