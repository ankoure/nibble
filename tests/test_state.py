from __future__ import annotations

from datetime import datetime, timezone


from nibble.gtfs.static import StaticGTFS
from nibble.models import Position, Trip, VehicleEvent
from nibble.state import StateStore


def _pos() -> Position:
    return Position(latitude=42.0, longitude=-71.0)


def _event(
    vehicle_id: str = "v1",
    trip_id: str | None = "trip-1",
    ts: datetime | None = None,
) -> VehicleEvent:
    return VehicleEvent(
        vehicle_id=vehicle_id,
        trip_id=trip_id,
        position=_pos(),
        timestamp=ts or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


def _gtfs(trip_ids: list[str] | None = None) -> StaticGTFS:
    gtfs = StaticGTFS()
    for tid in trip_ids or ["trip-1"]:
        gtfs.trips[tid] = Trip(trip_id=tid, route_id=f"route-{tid}")
    return gtfs


class TestResolutionLadder:
    def test_known_trip_id_is_confirmed(self) -> None:
        store = StateStore()
        gtfs = _gtfs(["trip-1"])
        event = _event(trip_id="trip-1")
        result = store.update_from_event(event, gtfs, stale_threshold_seconds=90)
        assert result.confidence == "confirmed"
        assert result.provenance == "observed"
        assert result.trip_id == "trip-1"
        assert result.route_id == "route-trip-1"

    def test_unknown_trip_id_still_confirmed(self) -> None:
        store = StateStore()
        gtfs = _gtfs([])  # empty
        event = _event(trip_id="unknown-trip")
        result = store.update_from_event(event, gtfs, stale_threshold_seconds=90)
        assert result.confidence == "confirmed"
        assert result.trip_id == "unknown-trip"

    def test_missing_trip_id_within_threshold_is_inferred(self) -> None:
        store = StateStore()
        gtfs = _gtfs(["trip-1"])
        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)  # 60s later

        # First: establish valid state
        store.update_from_event(_event(trip_id="trip-1", ts=t0), gtfs, stale_threshold_seconds=90)

        # Second: no trip_id, within 90s
        result = store.update_from_event(
            _event(trip_id=None, ts=t1), gtfs, stale_threshold_seconds=90
        )
        assert result.confidence == "inferred"
        assert result.provenance == "inferred"
        assert result.trip_id == "trip-1"

    def test_missing_trip_id_beyond_threshold_is_stale(self) -> None:
        store = StateStore()
        gtfs = _gtfs(["trip-1"])
        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 2, 0, tzinfo=timezone.utc)  # 120s later

        store.update_from_event(_event(trip_id="trip-1", ts=t0), gtfs, stale_threshold_seconds=90)
        result = store.update_from_event(
            _event(trip_id=None, ts=t1), gtfs, stale_threshold_seconds=90
        )
        assert result.confidence == "stale"

    def test_never_seen_vehicle_with_no_trip_id_is_stale(self) -> None:
        store = StateStore()
        gtfs = _gtfs([])
        result = store.update_from_event(_event(trip_id=None), gtfs, stale_threshold_seconds=90)
        assert result.confidence == "stale"

    def test_remove_clears_state(self) -> None:
        store = StateStore()
        gtfs = _gtfs(["trip-1"])
        store.update_from_event(_event(trip_id="trip-1"), gtfs, stale_threshold_seconds=90)
        assert store.get("v1") is not None
        store.remove("v1")
        assert store.get("v1") is None
