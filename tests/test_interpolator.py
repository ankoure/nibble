from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest


from nibble.gtfs.static import StaticGTFS
from nibble.interpolator import _scheduled_durations, _stop_bearing, interpolate
from nibble.models import Position, StopTime, Trip, VehicleEvent
from nibble.state import VehicleState


def _gtfs_with_stops_and_stop_times(trip_id: str = "trip-1", n_stops: int = 10) -> StaticGTFS:
    """Like _gtfs_with_stop_times but also populates gtfs.stops with predictable coordinates."""
    gtfs = _gtfs_with_stop_times(trip_id, n_stops)
    for i in range(1, n_stops + 1):
        gtfs.stops[f"stop-{i}"] = (42.0 + i * 0.01, -71.0 + i * 0.01)
    return gtfs


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

    # --- Issue 1: max_stops uses actual stop count, not sequence delta ---

    def test_noncontiguous_sequences_within_max_stops(self) -> None:
        """seq_delta=4 but only 2 actual intermediate stops — should not be rejected."""
        gtfs = StaticGTFS()
        gtfs.trips["trip-1"] = Trip(trip_id="trip-1", route_id="route-1")
        gtfs.stop_times["trip-1"] = [
            StopTime("trip-1", "stop-3", 3, "12:03:00", "12:03:30"),
            StopTime("trip-1", "stop-5", 5, "12:05:00", "12:05:30"),
            StopTime("trip-1", "stop-7", 7, "12:07:00", "12:07:30"),
        ]
        prev = _state(seq=3, ts=datetime(2024, 1, 1, 12, 3, 30, tzinfo=timezone.utc))
        curr = _curr_event(seq=7, ts=datetime(2024, 1, 1, 12, 7, 30, tzinfo=timezone.utc))
        events = interpolate(prev, curr, gtfs, max_stops=3)
        assert len(events) == 2
        assert [e.current_stop_sequence for e in events] == [5, 7]

    def test_noncontiguous_sequences_exceeding_max_stops(self) -> None:
        """4 actual intermediate stops should be rejected when max_stops=3."""
        gtfs = StaticGTFS()
        gtfs.trips["trip-1"] = Trip(trip_id="trip-1", route_id="route-1")
        gtfs.stop_times["trip-1"] = [
            StopTime("trip-1", "s1", 1, "12:01:00", "12:01:30"),
            StopTime("trip-1", "s2", 2, "12:02:00", "12:02:30"),
            StopTime("trip-1", "s3", 3, "12:03:00", "12:03:30"),
            StopTime("trip-1", "s4", 4, "12:04:00", "12:04:30"),
            StopTime("trip-1", "s5", 5, "12:05:00", "12:05:30"),
        ]
        prev = _state(seq=1, ts=datetime(2024, 1, 1, 12, 1, 30, tzinfo=timezone.utc))
        curr = _curr_event(seq=5, ts=datetime(2024, 1, 1, 12, 5, 30, tzinfo=timezone.utc))
        events = interpolate(prev, curr, gtfs, max_stops=3)
        assert events == []

    # --- Issue 2: diagnostic logging ---

    def test_trip_mismatch_logs_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(trip_id="trip-1")
        curr = _curr_event(trip_id="trip-2")
        with caplog.at_level(logging.DEBUG, logger="nibble.interpolator"):
            interpolate(prev, curr, gtfs, max_stops=5)
        assert any("trip mismatch" in r.message for r in caplog.records)

    def test_backwards_sequence_logs_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=6)
        curr = _curr_event(seq=3)
        with caplog.at_level(logging.DEBUG, logger="nibble.interpolator"):
            interpolate(prev, curr, gtfs, max_stops=5)
        assert any("backwards sequence" in r.message for r in caplog.records)

    def test_max_stops_exceeded_logs_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=1)
        curr = _curr_event(seq=8)
        with caplog.at_level(logging.DEBUG, logger="nibble.interpolator"):
            interpolate(prev, curr, gtfs, max_stops=3)
        assert any("skipping" in r.message for r in caplog.records)

    # --- Issue 6: non-positive total_seconds ---

    def test_negative_total_seconds_returns_empty(self) -> None:
        """prev timestamp after curr timestamp should produce no events."""
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=3, ts=datetime(2024, 1, 1, 12, 10, 0, tzinfo=timezone.utc))
        curr = _curr_event(seq=6, ts=datetime(2024, 1, 1, 12, 6, 30, tzinfo=timezone.utc))
        events = interpolate(prev, curr, gtfs, max_stops=5)
        assert events == []

    def test_zero_total_seconds_returns_empty(self) -> None:
        """Identical timestamps should produce no events."""
        ts = datetime(2024, 1, 1, 12, 6, 30, tzinfo=timezone.utc)
        gtfs = _gtfs_with_stop_times()
        prev = _state(seq=3, ts=ts)
        curr = _curr_event(seq=6, ts=ts)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        assert events == []

    # --- Position interpolation using stop coordinates ---

    def test_intermediate_positions_use_stop_coordinates(self) -> None:
        gtfs = _gtfs_with_stops_and_stop_times()
        prev = _state(seq=3)
        curr = _curr_event(seq=6)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        # stops 4 and 5 are intermediate; stop 6 is the observed final event
        for e in events[:-1]:
            assert e.stop_id is not None
            expected_lat, expected_lon = gtfs.stops[e.stop_id]
            assert e.position.latitude == expected_lat
            assert e.position.longitude == expected_lon
        # Final event keeps the observed GPS position unchanged
        assert events[-1].position is curr.position

    def test_intermediate_speed_is_zero(self) -> None:
        gtfs = _gtfs_with_stops_and_stop_times()
        prev = _state(seq=3)
        curr = _curr_event(seq=6)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        for e in events[:-1]:
            assert e.position.speed == 0.0

    def test_intermediate_bearing_points_toward_next_stop(self) -> None:
        gtfs = _gtfs_with_stops_and_stop_times()
        prev = _state(seq=3)
        curr = _curr_event(seq=6)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        # First intermediate event (stop-4) should bear toward stop-5
        stop4_lat, stop4_lon = gtfs.stops["stop-4"]
        stop5_lat, stop5_lon = gtfs.stops["stop-5"]
        expected = _stop_bearing(stop4_lat, stop4_lon, stop5_lat, stop5_lon)
        assert events[0].position.bearing is not None
        assert abs(events[0].position.bearing - expected) < 0.01

    def test_intermediate_position_fallback_when_stop_missing(self) -> None:
        """When gtfs.stops has no entry for a stop, fall back to curr.position."""
        gtfs = _gtfs_with_stop_times()  # no stops populated
        prev = _state(seq=3)
        curr = _curr_event(seq=6)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        for e in events[:-1]:
            assert e.position is curr.position

    def test_bearing_none_when_next_stop_missing(self) -> None:
        """When next stop's coordinates are absent, bearing should be None."""
        gtfs = _gtfs_with_stops_and_stop_times()
        # Remove stop-5 so the bearing from stop-4 toward stop-5 can't be computed
        del gtfs.stops["stop-5"]
        prev = _state(seq=3)
        curr = _curr_event(seq=6)
        events = interpolate(prev, curr, gtfs, max_stops=5)
        # stop-4 is intermediate, next stop is stop-5 (missing) → bearing is None
        stop4_event = next(e for e in events if e.stop_id == "stop-4")
        assert stop4_event.position.bearing is None


class TestScheduledDurations:
    def test_all_times_present_unchanged(self) -> None:
        stop_times = [
            StopTime("t", "s1", 1, "12:00:00", "12:00:00"),
            StopTime("t", "s2", 2, "12:02:00", "12:02:00"),
            StopTime("t", "s3", 3, "12:05:00", "12:05:00"),
        ]
        durations = _scheduled_durations(stop_times, prev_seq=1, curr_seq=3)
        assert abs(durations[0] - 120.0) < 1.0
        assert abs(durations[1] - 300.0) < 1.0

    def test_middle_stop_missing_time_interpolated(self) -> None:
        stop_times = [
            StopTime("t", "s1", 1, "12:00:00", "12:00:00"),
            StopTime("t", "s2", 2, None, None),
            StopTime("t", "s3", 3, "12:06:00", "12:06:00"),
        ]
        durations = _scheduled_durations(stop_times, prev_seq=1, curr_seq=3)
        assert len(durations) == 2
        assert abs(durations[0] - 180.0) < 1.0  # s2 at midpoint of 0–360s
        assert abs(durations[1] - 360.0) < 1.0  # s3 at 6 minutes

    def test_missing_base_stop_time_returns_empty(self) -> None:
        stop_times = [
            StopTime("t", "s1", 1, None, None),
            StopTime("t", "s2", 2, "12:03:00", "12:03:00"),
            StopTime("t", "s3", 3, "12:06:00", "12:06:00"),
        ]
        durations = _scheduled_durations(stop_times, prev_seq=1, curr_seq=3)
        assert durations == []
