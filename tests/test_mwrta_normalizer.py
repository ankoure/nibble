from __future__ import annotations

from nibble import unknown_routes
from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.mwrta import MwrtaNormalizer
from nibble.protos import gtfs_realtime_pb2


def _gtfs(route_trips: dict[str, object], route_short_names: dict[str, str]) -> StaticGTFS:
    gtfs = StaticGTFS()
    gtfs.route_trips = route_trips  # type: ignore[assignment]
    gtfs.route_short_names = route_short_names
    return gtfs


def _feed(route_id: str) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "e1"
    entity.vehicle.vehicle.id = "v1"
    entity.vehicle.trip.route_id = route_id
    return feed


class TestMwrtaNormalizer:
    def setup_method(self) -> None:
        self.normalizer = MwrtaNormalizer()
        unknown_routes.clear()

    def test_route_id_already_in_route_trips_unchanged(self) -> None:
        feed = _feed("uuid-123")
        gtfs = _gtfs(route_trips={"uuid-123": []}, route_short_names={})
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == "uuid-123"

    def test_short_name_remapped_to_uuid(self) -> None:
        feed = _feed("06")
        gtfs = _gtfs(route_trips={}, route_short_names={"06": "uuid-06"})
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == "uuid-06"

    def test_unknown_route_id_recorded_in_registry(self) -> None:
        feed = _feed("unknown-99")
        gtfs = _gtfs(route_trips={}, route_short_names={})
        self.normalizer.normalize(feed, gtfs)
        entries = unknown_routes.all_entries()
        assert any(e["route_id"] == "unknown-99" for e in entries)

    def test_empty_route_id_skipped(self) -> None:
        feed = _feed("")
        gtfs = _gtfs(route_trips={}, route_short_names={})
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == ""
        assert unknown_routes.all_entries() == []
