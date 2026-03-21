"""Tests for nibble.publishers.vehicle_positions."""

from __future__ import annotations

from datetime import datetime, timezone

import boto3
import pytest
from google.transit import gtfs_realtime_pb2
from moto import mock_aws

from nibble.models import Position, VehicleEvent
from nibble.publishers.vehicle_positions import _build_feed, publish_vehicle_positions

BUCKET = "test-vp-bucket"
KEY = "vehicle_positions.pb"
REGION = "us-east-1"


def _event(
    vehicle_id: str = "v1",
    trip_id: str | None = "trip-1",
    route_id: str | None = "route-1",
    stop_id: str | None = "stop-A",
    label: str | None = "101",
    bearing: float | None = 90.0,
    speed: float | None = 12.5,
    direction_id: int | None = 0,
    current_stop_sequence: int | None = 3,
    current_status: str = "STOPPED_AT",
) -> VehicleEvent:
    return VehicleEvent(
        vehicle_id=vehicle_id,
        trip_id=trip_id,
        route_id=route_id,
        stop_id=stop_id,
        label=label,
        direction_id=direction_id,
        current_stop_sequence=current_stop_sequence,
        current_status=current_status,
        position=Position(latitude=41.82, longitude=-71.41, bearing=bearing, speed=speed),
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


class TestBuildFeed:
    def test_empty_snapshot_produces_empty_feed(self) -> None:
        data = _build_feed({})
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        assert len(feed.entity) == 0

    def test_single_vehicle_round_trips(self) -> None:
        data = _build_feed({"v1": _event()})
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)

        assert len(feed.entity) == 1
        vp = feed.entity[0].vehicle
        assert vp.vehicle.id == "v1"
        assert vp.vehicle.label == "101"
        assert vp.trip.trip_id == "trip-1"
        assert vp.trip.route_id == "route-1"
        assert vp.trip.direction_id == 0
        assert vp.position.latitude == pytest.approx(41.82)
        assert vp.position.longitude == pytest.approx(-71.41)
        assert vp.position.bearing == pytest.approx(90.0)
        assert vp.position.speed == pytest.approx(12.5)
        assert vp.stop_id == "stop-A"
        assert vp.current_stop_sequence == 3
        assert vp.current_status == 1  # STOPPED_AT

    def test_optional_fields_omitted_when_none(self) -> None:
        data = _build_feed(
            {
                "v1": _event(
                    trip_id=None, route_id=None, stop_id=None, label=None, bearing=None, speed=None
                )
            }
        )
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        vp = feed.entity[0].vehicle
        assert vp.trip.trip_id == ""
        assert vp.trip.route_id == ""
        assert vp.vehicle.label == ""
        assert vp.position.bearing == pytest.approx(0.0)
        assert vp.position.speed == pytest.approx(0.0)

    def test_status_map_in_transit_to(self) -> None:
        data = _build_feed({"v1": _event(current_status="IN_TRANSIT_TO")})
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        assert feed.entity[0].vehicle.current_status == 2  # IN_TRANSIT_TO

    def test_status_map_incoming_at(self) -> None:
        data = _build_feed({"v1": _event(current_status="INCOMING_AT")})
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        assert feed.entity[0].vehicle.current_status == 0  # INCOMING_AT

    def test_multiple_vehicles(self) -> None:
        snapshot = {
            "v1": _event("v1", trip_id="trip-1"),
            "v2": _event("v2", trip_id="trip-2"),
        }
        data = _build_feed(snapshot)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        assert len(feed.entity) == 2
        ids = {e.vehicle.vehicle.id for e in feed.entity}
        assert ids == {"v1", "v2"}


class TestPublishVehiclePositions:
    async def test_uploads_protobuf_to_s3(self) -> None:
        with mock_aws():
            s3 = boto3.client("s3", region_name=REGION)
            s3.create_bucket(Bucket=BUCKET)

            await publish_vehicle_positions({"v1": _event()}, BUCKET, KEY, region=REGION)

            resp = s3.get_object(Bucket=BUCKET, Key=KEY)
            content = resp["Body"].read()
            assert len(content) > 0

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(content)
            assert len(feed.entity) == 1
            assert feed.entity[0].vehicle.vehicle.id == "v1"

    async def test_empty_snapshot_still_uploads(self) -> None:
        with mock_aws():
            s3 = boto3.client("s3", region_name=REGION)
            s3.create_bucket(Bucket=BUCKET)

            await publish_vehicle_positions({}, BUCKET, KEY, region=REGION)

            resp = s3.get_object(Bucket=BUCKET, Key=KEY)
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(resp["Body"].read())
            assert len(feed.entity) == 0
