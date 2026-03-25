from __future__ import annotations

from nibble.gtfs.static import StaticGTFS
from nibble.models import Trip
from nibble.normalizer.cttransit import CttransitNormalizer
from nibble.protos import gtfs_realtime_pb2


def _gtfs(*trip_ids: str) -> StaticGTFS:
    gtfs = StaticGTFS()
    for tid in trip_ids:
        gtfs.trips[tid] = Trip(trip_id=tid, route_id=f"route-{tid}")
    return gtfs


def _feed(trip_id: str, route_id: str = "") -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "e1"
    entity.vehicle.vehicle.id = "v1"
    entity.vehicle.trip.trip_id = trip_id
    if route_id:
        entity.vehicle.trip.route_id = route_id
    return feed


class TestCttransitNormalizer:
    def setup_method(self) -> None:
        self.normalizer = CttransitNormalizer()

    def test_fills_route_id_from_static_gtfs(self) -> None:
        feed = _feed("trip-1")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == "route-trip-1"

    def test_existing_route_id_not_overwritten(self) -> None:
        feed = _feed("trip-1", route_id="already-set")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == "already-set"

    def test_trip_id_not_in_gtfs_leaves_route_id_empty(self) -> None:
        feed = _feed("unknown-trip")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == ""

    def test_empty_trip_id_skipped(self) -> None:
        feed = _feed("")
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == ""

    def test_entity_without_vehicle_field_not_crashed(self) -> None:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        entity = feed.entity.add()
        entity.id = "alert-1"
        gtfs = _gtfs("trip-1")
        result = self.normalizer.normalize(feed, gtfs)
        assert not result.entity[0].HasField("vehicle")
