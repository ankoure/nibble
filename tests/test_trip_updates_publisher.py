from __future__ import annotations

from datetime import datetime, timezone

from nibble.gtfs.static import StaticGTFS
from nibble.models import Position, StopTime, Trip, VehicleEvent
from nibble.publishers.trip_updates import _build_feed


def _gtfs(trip_id: str = "trip-1", n_stops: int = 5) -> StaticGTFS:
    gtfs = StaticGTFS()
    gtfs.trips[trip_id] = Trip(trip_id=trip_id, route_id="route-1", direction_id=0)
    gtfs.stop_times[trip_id] = [
        StopTime(
            trip_id=trip_id,
            stop_id=f"stop-{i}",
            stop_sequence=i,
            arrival_time=f"12:{i:02d}:00",
            departure_time=f"12:{i:02d}:30",
        )
        for i in range(1, n_stops + 1)
    ]
    return gtfs


def _event(
    trip_id: str = "trip-1",
    seq: int = 2,
    ts: datetime | None = None,
) -> VehicleEvent:
    return VehicleEvent(
        vehicle_id="v1",
        trip_id=trip_id,
        route_id="route-1",
        direction_id=0,
        stop_id=f"stop-{seq}",
        current_stop_sequence=seq,
        label="Bus 42",
        position=Position(latitude=42.0, longitude=-71.0),
        timestamp=ts or datetime(2024, 1, 1, 12, 2, 30, tzinfo=timezone.utc),
    )


def _parse(pb_bytes: bytes):
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_bytes)
    return feed


class TestBuildFeed:
    def test_produces_entity_per_vehicle(self):
        gtfs = _gtfs()
        snapshot = {"v1": _event(seq=2, ts=datetime(2024, 1, 1, 12, 3, 30, tzinfo=timezone.utc))}
        feed = _parse(_build_feed(snapshot, gtfs))
        assert len(feed.entity) == 1

    def test_trip_descriptor_fields(self):
        gtfs = _gtfs()
        snapshot = {"v1": _event(seq=2, ts=datetime(2024, 1, 1, 12, 2, 30, tzinfo=timezone.utc))}
        feed = _parse(_build_feed(snapshot, gtfs))
        tu = feed.entity[0].trip_update
        assert tu.trip.trip_id == "trip-1"
        assert tu.trip.route_id == "route-1"
        assert tu.trip.direction_id == 0

    def test_vehicle_label(self):
        gtfs = _gtfs()
        snapshot = {"v1": _event(seq=2)}
        feed = _parse(_build_feed(snapshot, gtfs))
        assert feed.entity[0].trip_update.vehicle.label == "Bus 42"

    def test_remaining_stops_only(self):
        # Vehicle at stop-3; should include stops 3, 4, 5
        gtfs = _gtfs()
        snapshot = {"v1": _event(seq=3, ts=datetime(2024, 1, 1, 12, 3, 30, tzinfo=timezone.utc))}
        feed = _parse(_build_feed(snapshot, gtfs))
        stu_seqs = [stu.stop_sequence for stu in feed.entity[0].trip_update.stop_time_update]
        assert stu_seqs == [3, 4, 5]

    def test_delay_propagated(self):
        # stop-2 departs 12:02:30; vehicle at 12:03:30 → 60s late
        gtfs = _gtfs()
        snapshot = {"v1": _event(seq=2, ts=datetime(2024, 1, 1, 12, 3, 30, tzinfo=timezone.utc))}
        feed = _parse(_build_feed(snapshot, gtfs))
        for stu in feed.entity[0].trip_update.stop_time_update:
            assert stu.arrival.delay == 60
            assert stu.departure.delay == 60

    def test_skips_vehicle_without_trip_id(self):
        gtfs = _gtfs()
        event = VehicleEvent(
            vehicle_id="v2",
            trip_id=None,
            route_id="route-1",
            position=Position(latitude=42.0, longitude=-71.0),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        snapshot = {"v2": event}
        feed = _parse(_build_feed(snapshot, gtfs))
        assert len(feed.entity) == 0

    def test_skips_vehicle_without_stop_times(self):
        gtfs = StaticGTFS()
        snapshot = {"v1": _event()}
        feed = _parse(_build_feed(snapshot, gtfs))
        assert len(feed.entity) == 0

    def test_empty_snapshot(self):
        gtfs = _gtfs()
        feed = _parse(_build_feed({}, gtfs))
        assert len(feed.entity) == 0

    def test_header_version(self):
        gtfs = _gtfs()
        feed = _parse(_build_feed({}, gtfs))
        assert feed.header.gtfs_realtime_version == "2.0"
