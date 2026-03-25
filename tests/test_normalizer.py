from __future__ import annotations

from nibble.gtfs.static import StaticGTFS
from nibble.models import Trip
from nibble.normalizer.ripta import RiptaNormalizer
from nibble.protos import gtfs_realtime_pb2


def _gtfs(*trip_ids: str) -> StaticGTFS:
    gtfs = StaticGTFS()
    for tid in trip_ids:
        gtfs.trips[tid] = Trip(trip_id=tid, route_id=f"route-{tid}")
    return gtfs


def _feed(trip_id: str) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "v1"
    entity.vehicle.trip.trip_id = trip_id
    return feed


def _get_trip_id(feed: gtfs_realtime_pb2.FeedMessage) -> str:
    return str(feed.entity[0].vehicle.trip.trip_id)


class TestRiptaNormalizer:
    def setup_method(self) -> None:
        self.normalizer = RiptaNormalizer()

    def test_trip_already_in_gtfs_unchanged(self) -> None:
        feed = _feed("trip-1")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert _get_trip_id(result) == "trip-1"

    def test_suffix_stripped_when_prefix_in_gtfs(self) -> None:
        feed = _feed("trip-1_20240101")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert _get_trip_id(result) == "trip-1"

    def test_suffix_not_stripped_when_prefix_not_in_gtfs(self) -> None:
        feed = _feed("trip-1_20240101")
        gtfs = _gtfs("trip-99")  # prefix "trip-1" not in GTFS
        result = self.normalizer.normalize(feed, gtfs)
        assert _get_trip_id(result) == "trip-1_20240101"

    def test_no_underscore_not_in_gtfs_unchanged(self) -> None:
        feed = _feed("unknown-trip")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert _get_trip_id(result) == "unknown-trip"

    def test_multiple_underscores_uses_first_segment_only(self) -> None:
        feed = _feed("trip-1_extra_20240101")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert _get_trip_id(result) == "trip-1"

    def test_empty_trip_id_unchanged(self) -> None:
        feed = _feed("")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert _get_trip_id(result) == ""

    def test_entity_without_trip_field_unchanged(self) -> None:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        entity = feed.entity.add()
        entity.id = "v1"
        # Set only position, no trip field
        entity.vehicle.position.latitude = 41.8
        entity.vehicle.position.longitude = -71.4
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        # Should not crash and trip should remain unset
        assert not result.entity[0].vehicle.HasField("trip")

    def test_entity_without_vehicle_field_unchanged(self) -> None:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        entity = feed.entity.add()
        entity.id = "alert-1"
        # No vehicle field at all
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert not result.entity[0].HasField("vehicle")

    def test_multiple_vehicles_each_handled_independently(self) -> None:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        e1 = feed.entity.add()
        e1.id = "v1"
        e1.vehicle.trip.trip_id = "trip-1_20240101"
        e2 = feed.entity.add()
        e2.id = "v2"
        e2.vehicle.trip.trip_id = "trip-2_20240101"
        gtfs = _gtfs("trip-1")  # only trip-1 prefix is in GTFS
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.trip_id == "trip-1"
        assert result.entity[1].vehicle.trip.trip_id == "trip-2_20240101"
