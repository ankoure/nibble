from __future__ import annotations

from datetime import datetime, timezone

from nibble.gtfs.static import StaticGTFS
from nibble.models import Position, StopTime, Trip, VehicleEvent
from nibble.predictions import compute_delay, predict_arrivals


def _gtfs(trip_id: str = "trip-1", n_stops: int = 5) -> StaticGTFS:
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


def _event(
    trip_id: str = "trip-1",
    seq: int = 2,
    ts: datetime | None = None,
) -> VehicleEvent:
    return VehicleEvent(
        vehicle_id="v1",
        trip_id=trip_id,
        route_id="route-1",
        stop_id=f"stop-{seq}",
        current_stop_sequence=seq,
        position=Position(latitude=42.0, longitude=-71.0),
        timestamp=ts or datetime(2024, 1, 1, 12, 3, 0, tzinfo=timezone.utc),
    )


class TestComputeDelay:
    def test_on_time(self):
        # stop-2 departs at 12:02:30; vehicle timestamp 12:02:30 → 0 delay
        gtfs = _gtfs()
        event = _event(seq=2, ts=datetime(2024, 1, 1, 12, 2, 30, tzinfo=timezone.utc))
        assert compute_delay(event, gtfs) == 0

    def test_late(self):
        # stop-2 departs at 12:02:30; vehicle at 12:03:00 → +30s delay
        gtfs = _gtfs()
        event = _event(seq=2, ts=datetime(2024, 1, 1, 12, 3, 0, tzinfo=timezone.utc))
        assert compute_delay(event, gtfs) == 30

    def test_early(self):
        # stop-2 departs at 12:02:30; vehicle at 12:02:00 → -30s delay
        gtfs = _gtfs()
        event = _event(seq=2, ts=datetime(2024, 1, 1, 12, 2, 0, tzinfo=timezone.utc))
        assert compute_delay(event, gtfs) == -30

    def test_no_trip_id(self):
        gtfs = _gtfs()
        event = _event()
        event = VehicleEvent(
            vehicle_id="v1",
            trip_id=None,
            route_id="route-1",
            position=Position(latitude=42.0, longitude=-71.0),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        assert compute_delay(event, gtfs) is None

    def test_no_stop_sequence(self):
        gtfs = _gtfs()
        event = VehicleEvent(
            vehicle_id="v1",
            trip_id="trip-1",
            route_id="route-1",
            current_stop_sequence=None,
            position=Position(latitude=42.0, longitude=-71.0),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        assert compute_delay(event, gtfs) is None

    def test_trip_not_in_gtfs(self):
        gtfs = StaticGTFS()
        event = _event()
        assert compute_delay(event, gtfs) is None

    def test_stop_sequence_not_in_stop_times(self):
        gtfs = _gtfs()
        event = _event(seq=99)
        assert compute_delay(event, gtfs) is None


class TestPredictArrivals:
    def test_returns_remaining_stops(self):
        # Vehicle at stop-2; should return stops 2-5
        gtfs = _gtfs()
        event = _event(seq=2, ts=datetime(2024, 1, 1, 12, 2, 30, tzinfo=timezone.utc))
        results = predict_arrivals(event, gtfs)
        assert len(results) == 4  # stops 2, 3, 4, 5
        assert results[0]["stop_id"] == "stop-2"
        assert results[-1]["stop_id"] == "stop-5"

    def test_delay_applied_to_all_stops(self):
        # 60s late
        gtfs = _gtfs()
        event = _event(seq=2, ts=datetime(2024, 1, 1, 12, 3, 30, tzinfo=timezone.utc))
        results = predict_arrivals(event, gtfs)
        for r in results:
            assert r["delay_seconds"] == 60

    def test_predicted_after_scheduled_when_late(self):
        gtfs = _gtfs()
        event = _event(seq=2, ts=datetime(2024, 1, 1, 12, 3, 30, tzinfo=timezone.utc))
        results = predict_arrivals(event, gtfs)
        for r in results:
            from datetime import datetime as dt

            sched = dt.fromisoformat(r["scheduled_arrival"])
            pred = dt.fromisoformat(r["predicted_arrival"])
            assert (pred - sched).total_seconds() == 60

    def test_empty_when_no_trip_id(self):
        gtfs = _gtfs()
        event = VehicleEvent(
            vehicle_id="v1",
            trip_id=None,
            route_id="route-1",
            position=Position(latitude=42.0, longitude=-71.0),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        assert predict_arrivals(event, gtfs) == []

    def test_empty_when_no_stop_times(self):
        gtfs = StaticGTFS()
        event = _event()
        assert predict_arrivals(event, gtfs) == []

    def test_stop_predictions_have_required_fields(self):
        gtfs = _gtfs()
        event = _event(seq=1, ts=datetime(2024, 1, 1, 12, 1, 30, tzinfo=timezone.utc))
        results = predict_arrivals(event, gtfs)
        assert results
        r = results[0]
        assert "stop_id" in r
        assert "stop_sequence" in r
        assert "scheduled_arrival" in r
        assert "predicted_arrival" in r
        assert "delay_seconds" in r
