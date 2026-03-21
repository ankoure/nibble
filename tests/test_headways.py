from __future__ import annotations

from datetime import datetime, timezone

import pytest

from nibble.gtfs.static import StaticGTFS
from nibble.headways import compute_headways
from nibble.models import Position, StopTime, Trip, VehicleEvent


def _gtfs() -> StaticGTFS:
    gtfs = StaticGTFS()
    for trip_id, route_id, dir_id in [
        ("trip-a", "route-1", 0),
        ("trip-b", "route-1", 0),
        ("trip-c", "route-1", 1),
    ]:
        gtfs.trips[trip_id] = Trip(trip_id=trip_id, route_id=route_id, direction_id=dir_id)
        gtfs.stop_times[trip_id] = [
            StopTime(
                trip_id=trip_id,
                stop_id=f"stop-{i}",
                stop_sequence=i,
                arrival_time=f"12:{i:02d}:00",
                departure_time=f"12:{i:02d}:30",
                shape_dist_traveled=float(i * 1000),
            )
            for i in range(1, 6)
        ]
    return gtfs


def _event(
    vehicle_id: str,
    trip_id: str,
    route_id: str,
    direction_id: int,
    seq: int,
) -> VehicleEvent:
    return VehicleEvent(
        vehicle_id=vehicle_id,
        trip_id=trip_id,
        route_id=route_id,
        direction_id=direction_id,
        stop_id=f"stop-{seq}",
        current_stop_sequence=seq,
        position=Position(latitude=42.0, longitude=-71.0),
        timestamp=datetime(2024, 1, 1, 12, seq, 30, tzinfo=timezone.utc),
    )


class TestComputeHeadways:
    def test_groups_by_direction(self):
        snapshot = {
            "v1": _event("v1", "trip-a", "route-1", 0, seq=4),
            "v2": _event("v2", "trip-b", "route-1", 0, seq=2),
            "v3": _event("v3", "trip-c", "route-1", 1, seq=3),
        }
        gtfs = _gtfs()
        result = compute_headways("route-1", snapshot, gtfs)
        directions = result["directions"]
        direction_ids = [d["direction_id"] for d in directions]
        assert 0 in direction_ids
        assert 1 in direction_ids

    def test_sorted_furthest_ahead_first(self):
        # v1 at seq=4 (shape_dist 4000), v2 at seq=2 (shape_dist 2000)
        snapshot = {
            "v1": _event("v1", "trip-a", "route-1", 0, seq=4),
            "v2": _event("v2", "trip-b", "route-1", 0, seq=2),
        }
        gtfs = _gtfs()
        result = compute_headways("route-1", snapshot, gtfs)
        dir0 = next(d for d in result["directions"] if d["direction_id"] == 0)
        vehicles = dir0["vehicles"]
        assert vehicles[0]["vehicle_id"] == "v1"
        assert vehicles[1]["vehicle_id"] == "v2"

    def test_lead_vehicle_has_null_gaps(self):
        snapshot = {
            "v1": _event("v1", "trip-a", "route-1", 0, seq=4),
            "v2": _event("v2", "trip-b", "route-1", 0, seq=2),
        }
        gtfs = _gtfs()
        result = compute_headways("route-1", snapshot, gtfs)
        dir0 = next(d for d in result["directions"] if d["direction_id"] == 0)
        lead = dir0["vehicles"][0]
        assert lead["gap_to_previous_meters"] is None
        assert lead["scheduled_gap_to_previous_seconds"] is None

    def test_gap_metrics_computed(self):
        # v1 at stop-4 (shape_dist 4000, sched 12:04:30)
        # v2 at stop-2 (shape_dist 2000, sched 12:02:30)
        snapshot = {
            "v1": _event("v1", "trip-a", "route-1", 0, seq=4),
            "v2": _event("v2", "trip-b", "route-1", 0, seq=2),
        }
        gtfs = _gtfs()
        result = compute_headways("route-1", snapshot, gtfs)
        dir0 = next(d for d in result["directions"] if d["direction_id"] == 0)
        follower = dir0["vehicles"][1]
        assert follower["gap_to_previous_meters"] == pytest.approx(2000.0)
        # sched gap: 12:04:30 - 12:02:30 = 120s
        assert follower["scheduled_gap_to_previous_seconds"] == 120

    def test_no_vehicles_on_route(self):
        snapshot = {}
        gtfs = _gtfs()
        result = compute_headways("route-1", snapshot, gtfs)
        assert result["route_id"] == "route-1"
        assert result["directions"] == []

    def test_filters_to_requested_route(self):
        snapshot = {
            "v1": _event("v1", "trip-a", "route-1", 0, seq=3),
            "v2": _event("v2", "trip-b", "route-2", 0, seq=3),
        }
        snapshot["v2"] = VehicleEvent(
            vehicle_id="v2",
            trip_id="trip-x",
            route_id="route-2",
            direction_id=0,
            current_stop_sequence=3,
            position=Position(latitude=42.0, longitude=-71.0),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        gtfs = _gtfs()
        result = compute_headways("route-1", snapshot, gtfs)
        all_vehicle_ids = [v["vehicle_id"] for d in result["directions"] for v in d["vehicles"]]
        assert "v1" in all_vehicle_ids
        assert "v2" not in all_vehicle_ids

    def test_single_vehicle_no_gaps(self):
        snapshot = {"v1": _event("v1", "trip-a", "route-1", 0, seq=3)}
        gtfs = _gtfs()
        result = compute_headways("route-1", snapshot, gtfs)
        dir0 = result["directions"][0]
        assert len(dir0["vehicles"]) == 1
        assert dir0["vehicles"][0]["gap_to_previous_meters"] is None
