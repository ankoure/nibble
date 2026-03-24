from __future__ import annotations

from nibble.protos import gtfs_realtime_pb2

from nibble import unknown_routes
from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.vta import VtaNormalizer


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


class TestVtaNormalizer:
    def setup_method(self) -> None:
        self.normalizer = VtaNormalizer()
        unknown_routes.clear()

    def test_route_id_already_in_route_trips_unchanged(self) -> None:
        feed = _feed("2801")
        gtfs = _gtfs(route_trips={"2801": []}, route_short_names={})
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == "2801"

    def test_short_name_remapped_to_internal_id(self) -> None:
        feed = _feed("3")
        gtfs = _gtfs(route_trips={}, route_short_names={"3": "2801"})
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == "2801"

    def test_unknown_route_id_recorded_in_registry(self) -> None:
        feed = _feed("unknown-X")
        gtfs = _gtfs(route_trips={}, route_short_names={})
        self.normalizer.normalize(feed, gtfs)
        entries = unknown_routes.all_entries()
        assert any(e["route_id"] == "unknown-X" for e in entries)

    def test_empty_route_id_skipped(self) -> None:
        feed = _feed("")
        gtfs = _gtfs(route_trips={}, route_short_names={})
        result = self.normalizer.normalize(feed, gtfs)
        assert result.entity[0].vehicle.trip.route_id == ""
        assert unknown_routes.all_entries() == []
