"""Normalizer for MTA NYCT subway feeds."""

from __future__ import annotations

import logging

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer
from nibble.protos import gtfs_realtime_pb2
from nibble.protos.nyct import nyct_subway_pb2

logger = logging.getLogger(__name__)

# Maps NYCT cardinal direction enum values to GTFS direction_id.
# For NYCT: NORTH = uptown/Bronx-bound = direction_id 0.
#           SOUTH = downtown/Brooklyn-bound = direction_id 1.
# EAST/WEST are not currently used per the proto spec.
_NYCT_DIRECTION_TO_GTFS: dict[int, int] = {
    nyct_subway_pb2.NyctTripDescriptor.NORTH: 0,
    nyct_subway_pb2.NyctTripDescriptor.SOUTH: 1,
}


class NyctNormalizer(BaseNormalizer):
    """Normalizer for MTA NYCT subway GTFS-RT feeds.

    The NYCT static GTFS uses full trip IDs that include a service-period
    prefix (e.g. ``"BFA25GEN-B087-Weekday-00_067600_B..S65R"``), while the
    GTFS-RT feed publishes only the suffix (e.g. ``"067600_B..S65R"``).

    On first use with a given static GTFS, this normalizer builds a suffix
    index mapping each short trip ID to its full static counterpart. Each
    incoming RT trip ID that matches a suffix is rewritten to the full form
    so that downstream trip lookups (stop times, interpolation) succeed.

    Trip IDs with no suffix match (e.g. B-train express variants not present
    in the static GTFS) are left unchanged; the state machine handles them
    via its unknown-trip-id path.

    NYCT extension fields processed per entity:

    - ``is_assigned``: Entities with ``is_assigned=False`` are dropped — they
      are phantom scheduled trips with no physical train assigned and produce
      spurious unknown-trip-id warnings downstream.
    - ``train_id``: Physical train/consist identifier from ATS. Written to
      ``vehicle.vehicle.id`` when the feed leaves that field empty.
    - ``direction``: Cardinal direction (NORTH/SOUTH) mapped to GTFS
      ``direction_id`` (0/1) when the trip descriptor does not already carry one.
    """

    def __init__(self) -> None:
        self._suffix_index: dict[str, str] = {}
        self._gtfs_id: int | None = None

    def _build_index(self, gtfs: StaticGTFS) -> None:
        """Rebuild the suffix → full trip ID index when the static GTFS changes."""
        if id(gtfs) == self._gtfs_id:
            return
        index: dict[str, str] = {}
        for full_id in gtfs.trips:
            # Full NYCT trip IDs have the form "{service_period}_{short_id}".
            # Split on the first underscore to isolate the short suffix.
            idx = full_id.find("_")
            if idx == -1:
                continue
            short_id = full_id[idx + 1 :]
            if short_id not in index:
                index[short_id] = full_id
        self._suffix_index = index
        self._gtfs_id = id(gtfs)
        logger.debug("NYCT: built suffix index with %d entries", len(index))

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        """Rewrite short NYCT trip IDs to their full static GTFS form, drop
        unassigned trips, and back-fill direction_id and vehicle ID from
        NYCT-specific protobuf extensions.

        Args:
            feed: The raw ``FeedMessage`` from the NYCT GTFS-RT feed.
            gtfs: Static GTFS indexes used to build the suffix lookup.

        Returns:
            The modified ``FeedMessage`` with:

            - Entities with ``is_assigned=False`` removed.
            - Trip IDs expanded to full form where a static match exists.
            - ``direction_id`` set from the NYCT cardinal direction when absent.
            - ``vehicle.id`` populated from ``train_id`` when absent.
        """
        self._build_index(gtfs)

        keep = []
        for entity in feed.entity:
            if entity.HasField("vehicle"):
                vehicle = entity.vehicle

                if vehicle.HasField("trip") and vehicle.trip.HasExtension(
                    nyct_subway_pb2.nyct_trip_descriptor
                ):
                    ext = vehicle.trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor]

                    # Drop phantom trips: scheduled entries with no physical train.
                    if ext.HasField("is_assigned") and not ext.is_assigned:
                        logger.debug("NYCT: dropping unassigned trip entity %s", entity.id)
                        continue

                    # Back-fill vehicle.id from train_id when the feed omits it.
                    if ext.HasField("train_id") and ext.train_id:
                        if not (vehicle.HasField("vehicle") and vehicle.vehicle.id):
                            vehicle.vehicle.id = ext.train_id
                            logger.debug(
                                "NYCT: set vehicle.id=%r from train_id for entity %s",
                                ext.train_id,
                                entity.id,
                            )

                    # Back-fill direction_id from cardinal direction when absent.
                    if ext.HasField("direction") and not vehicle.trip.HasField("direction_id"):
                        gtfs_dir = _NYCT_DIRECTION_TO_GTFS.get(ext.direction)
                        if gtfs_dir is not None:
                            vehicle.trip.direction_id = gtfs_dir

                # Trip ID suffix rewrite.
                if vehicle.HasField("trip"):
                    trip_id = vehicle.trip.trip_id
                    if trip_id and trip_id not in gtfs.trips:
                        full_id = self._suffix_index.get(trip_id)
                        if full_id:
                            logger.debug(
                                "NYCT: rewrote trip_id %r -> %r", trip_id, full_id
                            )
                            vehicle.trip.trip_id = full_id

            keep.append(entity)

        if len(keep) != len(feed.entity):
            saved = list(keep)
            del feed.entity[:]
            feed.entity.extend(saved)

        return feed
