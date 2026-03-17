from __future__ import annotations

from datetime import datetime, timezone

import pytest
from google.transit import gtfs_realtime_pb2

from nibble.poller import _parse_feed

FEED_TS = 1704067200  # 2024-01-01 00:00:00 UTC
VEHICLE_TS = 1704067260  # 2024-01-01 00:01:00 UTC


def _feed(header_ts: int = FEED_TS) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = header_ts
    return feed


def _add_vehicle(
    feed: gtfs_realtime_pb2.FeedMessage,
    *,
    entity_id: str = "e1",
    vehicle_id: str = "v1",
    trip_id: str | None = "trip-1",
    route_id: str | None = "route-1",
    lat: float = 41.8,
    lon: float = -71.4,
    bearing: float | None = 90.0,
    speed: float | None = 5.0,
    timestamp: int = VEHICLE_TS,
    stop_id: str = "stop-1",
    stop_seq: int = 3,
    status: int = 2,  # IN_TRANSIT_TO
    label: str = "Bus 42",
) -> None:
    entity = feed.entity.add()
    entity.id = entity_id
    v = entity.vehicle
    v.vehicle.id = vehicle_id
    v.vehicle.label = label
    if trip_id is not None:
        v.trip.trip_id = trip_id
    if route_id is not None:
        v.trip.route_id = route_id
    v.position.latitude = lat
    v.position.longitude = lon
    if bearing is not None:
        v.position.bearing = bearing
    if speed is not None:
        v.position.speed = speed
    v.timestamp = timestamp
    v.stop_id = stop_id
    v.current_stop_sequence = stop_seq
    v.current_status = status


class TestParseFeed:
    def test_normal_vehicle_parsed_correctly(self) -> None:
        feed = _feed()
        _add_vehicle(feed)
        snap = _parse_feed(feed)
        assert "v1" in snap
        e = snap["v1"]
        assert e.vehicle_id == "v1"
        assert e.trip_id == "trip-1"
        assert e.route_id == "route-1"
        assert e.position.latitude == pytest.approx(41.8, rel=1e-5)
        assert e.position.longitude == pytest.approx(-71.4, rel=1e-5)
        assert e.position.bearing == pytest.approx(90.0)
        assert e.position.speed == pytest.approx(5.0)
        expected_ts = datetime.fromtimestamp(VEHICLE_TS, tz=timezone.utc)
        assert e.timestamp == expected_ts
        assert e.stop_id == "stop-1"
        assert e.current_stop_sequence == 3
        assert e.current_status == "IN_TRANSIT_TO"
        assert e.label == "Bus 42"

    def test_entity_without_vehicle_field_skipped(self) -> None:
        feed = _feed()
        entity = feed.entity.add()
        entity.id = "alert-1"
        # no vehicle field
        snap = _parse_feed(feed)
        assert snap == {}

    def test_vehicle_without_trip_field_has_none_trip_id(self) -> None:
        feed = _feed()
        entity = feed.entity.add()
        entity.id = "e1"
        entity.vehicle.vehicle.id = "v1"
        entity.vehicle.position.latitude = 41.8
        entity.vehicle.position.longitude = -71.4
        entity.vehicle.timestamp = VEHICLE_TS
        snap = _parse_feed(feed)
        assert snap["v1"].trip_id is None
        assert snap["v1"].route_id is None

    def test_vehicle_without_position_field_defaults_to_zero(self) -> None:
        feed = _feed()
        entity = feed.entity.add()
        entity.id = "e1"
        entity.vehicle.vehicle.id = "v1"
        entity.vehicle.trip.trip_id = "trip-1"
        entity.vehicle.timestamp = VEHICLE_TS
        snap = _parse_feed(feed)
        assert snap["v1"].position.latitude == 0.0
        assert snap["v1"].position.longitude == 0.0
        assert snap["v1"].position.bearing is None
        assert snap["v1"].position.speed is None

    def test_bearing_zero_becomes_none(self) -> None:
        # bearing=0.0 (due north) is falsy - treated as None by _parse_feed
        feed = _feed()
        _add_vehicle(feed, bearing=0.0)
        snap = _parse_feed(feed)
        assert snap["v1"].position.bearing is None

    def test_speed_zero_becomes_none(self) -> None:
        # speed=0.0 (stopped) is falsy - treated as None by _parse_feed
        feed = _feed()
        _add_vehicle(feed, speed=0.0)
        snap = _parse_feed(feed)
        assert snap["v1"].position.speed is None

    def test_vehicle_id_falls_back_to_entity_id(self) -> None:
        feed = _feed()
        entity = feed.entity.add()
        entity.id = "fallback-id"
        entity.vehicle.trip.trip_id = "trip-1"
        entity.vehicle.timestamp = VEHICLE_TS
        # vehicle.id is empty string (not set) → falls back to entity.id
        snap = _parse_feed(feed)
        assert "fallback-id" in snap

    def test_empty_vehicle_id_and_entity_id_skipped(self) -> None:
        feed = _feed()
        entity = feed.entity.add()
        entity.id = ""
        entity.vehicle.trip.trip_id = "trip-1"
        snap = _parse_feed(feed)
        assert snap == {}

    def test_vehicle_timestamp_zero_uses_feed_timestamp(self) -> None:
        feed = _feed(header_ts=FEED_TS)
        _add_vehicle(feed, timestamp=0)
        snap = _parse_feed(feed)
        expected = datetime.fromtimestamp(FEED_TS, tz=timezone.utc)
        assert snap["v1"].timestamp == expected

    def test_feed_timestamp_zero_uses_now(self) -> None:
        feed = _feed(header_ts=0)
        before = datetime.now(timezone.utc)
        _add_vehicle(feed, timestamp=0)
        snap = _parse_feed(feed)
        after = datetime.now(timezone.utc)
        assert before <= snap["v1"].timestamp <= after

    def test_status_incoming_at(self) -> None:
        feed = _feed()
        _add_vehicle(feed, status=0)
        snap = _parse_feed(feed)
        assert snap["v1"].current_status == "INCOMING_AT"

    def test_status_stopped_at(self) -> None:
        feed = _feed()
        _add_vehicle(feed, status=1)
        snap = _parse_feed(feed)
        assert snap["v1"].current_status == "STOPPED_AT"

    def test_multiple_vehicles_all_parsed(self) -> None:
        feed = _feed()
        _add_vehicle(feed, entity_id="e1", vehicle_id="v1")
        _add_vehicle(feed, entity_id="e2", vehicle_id="v2", trip_id="trip-2")
        snap = _parse_feed(feed)
        assert set(snap.keys()) == {"v1", "v2"}
        assert snap["v2"].trip_id == "trip-2"
