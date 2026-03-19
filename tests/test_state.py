from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

from nibble.gtfs.static import StaticGTFS
from nibble.models import Position, StopTime, Trip, VehicleEvent
from nibble.overrides import OverrideStore
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

    def test_direction_id_carried_forward_when_trip_id_missing(self) -> None:
        """direction_id from prior valid state should survive the inferred carry-forward."""
        store = StateStore()
        gtfs = StaticGTFS()
        gtfs.trips["trip-1"] = Trip(trip_id="trip-1", route_id="route-1", direction_id=1)
        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)

        store.update_from_event(
            VehicleEvent(
                vehicle_id="v1",
                trip_id="trip-1",
                direction_id=1,
                position=_pos(),
                timestamp=t0,
            ),
            gtfs,
            stale_threshold_seconds=90,
        )
        result = store.update_from_event(
            VehicleEvent(
                vehicle_id="v1",
                trip_id=None,
                direction_id=None,
                position=_pos(),
                timestamp=t1,
            ),
            gtfs,
            stale_threshold_seconds=90,
        )
        assert result.confidence == "inferred"
        assert result.direction_id == 1


def _gtfs_with_stop_times(trip_id: str, num_stops: int) -> StaticGTFS:
    """Build a StaticGTFS with *num_stops* consecutive stops for *trip_id*."""
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
        for i in range(1, num_stops + 1)
    ]
    return gtfs


class TestOverrideAutoExpiry:
    def _store_with_override(
        self, trip_id: str, vehicle_id: str = "v1"
    ) -> tuple[StateStore, OverrideStore, StaticGTFS]:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            overrides_path = Path(f.name)
        overrides = OverrideStore(overrides_path)
        overrides.set(vehicle_id, trip_id)
        gtfs = _gtfs_with_stop_times(trip_id, num_stops=3)
        store = StateStore(overrides=overrides)
        return store, overrides, gtfs

    def test_override_applied_before_last_stop(self) -> None:
        store, overrides, gtfs = self._store_with_override("trip-1")
        event = VehicleEvent(
            vehicle_id="v1",
            trip_id=None,
            position=_pos(),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            current_stop_sequence=2,  # stop 2 of 3 - not yet at end
        )
        result = store.update_from_event(event, gtfs, stale_threshold_seconds=90)
        assert result.trip_id == "trip-1"
        assert result.provenance == "manual"
        assert overrides.get("v1") == "trip-1"  # override still active

    def test_override_expires_at_last_stop(self) -> None:
        store, overrides, gtfs = self._store_with_override("trip-1")
        event = VehicleEvent(
            vehicle_id="v1",
            trip_id=None,
            position=_pos(),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            current_stop_sequence=3,  # last stop of 3-stop trip
        )
        store.update_from_event(event, gtfs, stale_threshold_seconds=90)
        # Override should have been cleared
        assert overrides.get("v1") is None

    def test_override_not_set_means_no_effect(self) -> None:
        store = StateStore()
        gtfs = _gtfs_with_stop_times("trip-1", num_stops=3)
        event = VehicleEvent(
            vehicle_id="v1",
            trip_id=None,
            position=_pos(),
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        result = store.update_from_event(event, gtfs, stale_threshold_seconds=90)
        assert result.provenance != "manual"
