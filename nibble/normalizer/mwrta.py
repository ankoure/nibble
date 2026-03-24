"""Normalizer for MWRTA (MetroWest Regional Transit Authority) feeds."""

from __future__ import annotations

import logging

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer
from nibble.protos import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


class MwrtaNormalizer(BaseNormalizer):
    """Normalizer for MWRTA feeds.

    MWRTA's static GTFS uses internal UUIDs for route_id, but the GTFS-RT feed
    reports human-readable short names (e.g. ``"06"``, ``"07"``).  This
    normalizer remaps the feed's route_id to the corresponding UUID using the
    ``route_short_names`` index built from ``routes.txt``.
    """

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        """Remap MWRTA route short names to their static GTFS UUID route_ids.

        Args:
            feed: The raw ``FeedMessage`` from the MWRTA feed.
            gtfs: Static GTFS indexes containing the ``route_short_names`` map.

        Returns:
            The modified ``FeedMessage`` with normalised route IDs.
        """
        logger.debug(
            "MWRTA normalizer: %d entities, route_trips keys sample: %s",
            len(feed.entity),
            ", ".join(list(gtfs.route_trips)[:5]),
        )
        for entity in feed.entity:
            # Avoid HasField - in proto3 accessing an unset message field returns
            # a default instance, and unset string fields return "".
            route_id = entity.vehicle.trip.route_id
            logger.debug(
                "MWRTA entity route_id=%r in_route_trips=%s", route_id, route_id in gtfs.route_trips
            )
            if not route_id or route_id in gtfs.route_trips:
                continue
            uuid = gtfs.route_short_names.get(route_id)
            if uuid:
                logger.info("MWRTA: remapped route_id %r -> %r", route_id, uuid)
                entity.vehicle.trip.route_id = uuid
            else:
                logger.warning(
                    "MWRTA: unknown route_id %r - not in routes.txt (known short names: %s)",
                    route_id,
                    ", ".join(sorted(gtfs.route_short_names)[:10]),
                )
                from nibble import unknown_routes

                unknown_routes.record(route_id)
        return feed
