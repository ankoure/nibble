from __future__ import annotations

from datetime import datetime, timezone


from nibble.gtfs.static import StaticGTFS
from nibble.interpolator import interpolate
from nibble.models import Position, StopTime, Trip, VehicleEvent
from nibble.state import VehicleState


def _gtfs_with_stop_times(trip_id: str = "trip-1", n_stops: int = 10) -> StaticGTFS:
    gtfs = StaticGTFS()
    gtfs.trips[trip_id] = Trip(trip_id=trip_id, route_id="route-1")
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


def _state(
    vehicle_id: str = "v1",
    trip_id: str | None = "trip-1",
    seq: int | None = 3,
    ts: datetime | None = None,
) -> VehicleState:
    return VehicleState(
        vehicle_id=vehicle_id,
        last_seen=ts or datetime(2024, 1, 1, 12, 3, 30, tzinfo=timezone.utc),
        confidence="confirmed",
        last_valid_trip_id=trip_id,
        last_valid_stop_sequence=seq,
        last_position=Position(latitude=42.0, longitude=-71.0),
    )


def _curr_event(
    vehicle_id: str = "v1",
    trip_id: str | None = "trip-1",
    seq: int = 6,
    ts: datetime | None = None,
) -> VehicleEvent:
    return VehicleEvent(
        vehicle_id=vehicle_id,
        trip_id=trip_id,
        current_stop_sequence=seq,
        position=Position(latitude=42.1, longitude=-71.1),
        timestamp=ts or datetime(2024, 1, 1, 12, 6, 30, tzinfo=timezone.utc),
        confidence="confirmed",
    )


class TestInterpolate:
    def test_produces_intermediate_events(self) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=3)
        curr = _curr_event(seq=6)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        # Should produce events for stops 4, 5, 6
        assert len(events) == 3
        seqs = [e.current_stop_sequence for e in events]
        assert seqs == [4, 5, 6]

    def test_intermediate_events_are_interpolated(self) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=3)
        curr = _curr_event(seq=6)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        # All but last should be interpolated
        for e in events[:-1]:
            assert e.provenance == "interpolated"
        assert events[-1].provenance == "observed"

    def test_timestamps_are_ordered(self) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=3, ts=datetime(2024, 1, 1, 12, 3, 0, tzinfo=timezone.utc))
        curr = _curr_event(seq=6, ts=datetime(2024, 1, 1, 12, 9, 0, tzinfo=timezone.utc))
        events = interpolate(prev, curr, gtfs, max_stops=5)
        timestamps = [e.timestamp for e in events]
        assert timestamps == sorted(timestamps)

    def test_gap_exceeding_max_stops_returns_empty(self) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=1)
        curr = _curr_event(seq=8)
        events = interpolate(prev, curr, gtfs, max_stops=3)
        assert events == []

    def test_trip_id_mismatch_returns_empty(self) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(trip_id="trip-1")
        curr = _curr_event(trip_id="trip-2")
        events = interpolate(prev, curr, gtfs, max_stops=5)
        assert events == []

    def test_backwards_stop_sequence_returns_empty(self) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=6)
        curr = _curr_event(seq=3)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        assert events == []

    def test_no_stop_times_falls_back_to_linear(self) -> None:
        gtfs = StaticGTFS()
        gtfs.trips["trip-1"] = Trip(trip_id="trip-1", route_id="route-1")
        # No stop_times for trip-1
        prev = _state(seq=3, ts=datetime(2024, 1, 1, 12, 3, 0, tzinfo=timezone.utc))
        curr = _curr_event(seq=5, ts=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc))
        events = interpolate(prev, curr, gtfs, max_stops=5)
        assert len(events) == 2
        assert events[-1].provenance == "observed"
