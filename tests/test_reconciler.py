from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from nibble.config import Settings
from nibble.gtfs.static import StaticGTFS
from nibble.models import Position, Trip, VehicleEvent
from nibble.reconciler import reconcile
from nibble.state import StateStore


def _settings(**kwargs: Any) -> Settings:
    defaults: dict[str, Any] = dict(
        gtfs_rt_url="http://example.com/rt",
        gtfs_static_url="http://example.com/static.zip",
        stale_vehicle_threshold_seconds=90,
        max_interpolation_stops=3,
    )
    defaults.update(kwargs)
    return Settings(**defaults)


def _gtfs(trip_ids: list[str] | None = None) -> StaticGTFS:
    gtfs = StaticGTFS()
    for tid in trip_ids or ["trip-1"]:
        gtfs.trips[tid] = Trip(trip_id=tid, route_id=f"route-{tid}")
    return gtfs


def _event(
    vehicle_id: str = "v1",
    trip_id: str | None = "trip-1",
    seq: int | None = 1,
    ts: datetime | None = None,
) -> VehicleEvent:
    return VehicleEvent(
        vehicle_id=vehicle_id,
        trip_id=trip_id,
        current_stop_sequence=seq,
        position=Position(latitude=42.0, longitude=-71.0),
        timestamp=ts or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


class TestFirstCall:
    def test_empty_prev_emits_reset(self) -> None:
        gtfs = _gtfs()
        config = _settings()
        store = StateStore()
        curr = {"v1": _event("v1")}
        events = reconcile({}, curr, store, gtfs, config)
        assert len(events) == 1
        assert events[0].event_type == "reset"
        assert any(d["id"] == "v1" for d in events[0].data)

    def test_reset_contains_all_vehicles(self) -> None:
        gtfs = _gtfs(["trip-1", "trip-2"])
        config = _settings()
        store = StateStore()
        curr = {
            "v1": _event("v1", "trip-1"),
            "v2": _event("v2", "trip-2"),
        }
        events = reconcile({}, curr, store, gtfs, config)
        assert len(events) == 1
        assert events[0].event_type == "reset"
        ids = {d["id"] for d in events[0].data}
        assert ids == {"v1", "v2"}


class TestSubsequentCalls:
    def test_new_vehicle_emits_add(self) -> None:
        gtfs = _gtfs()
        config = _settings()
        store = StateStore()
        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        prev = {"v1": _event("v1", seq=1, ts=t0)}
        curr = {
            "v1": _event("v1", seq=1, ts=t0),
            "v2": _event("v2", seq=1, ts=t1),
        }

        reconcile({}, prev, store, gtfs, config)
        events = reconcile(prev, curr, store, gtfs, config)
        add_events = [e for e in events if e.event_type == "add"]
        assert len(add_events) == 1
        assert add_events[0].data["id"] == "v2"

    def test_changed_vehicle_emits_update(self) -> None:
        gtfs = _gtfs(["trip-1", "trip-2"])
        config = _settings()
        store = StateStore()
        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        prev = {"v1": _event("v1", seq=1, ts=t0)}
        curr = {"v1": _event("v1", seq=2, ts=t1)}

        # Initialize state
        reconcile({}, prev, store, gtfs, config)
        events = reconcile(prev, curr, store, gtfs, config)
        update_events = [e for e in events if e.event_type == "update"]
        assert update_events

    def test_removed_vehicle_emits_remove(self) -> None:
        gtfs = _gtfs()
        config = _settings()
        store = StateStore()
        prev = {"v1": _event("v1")}
        curr: dict[str, VehicleEvent] = {}
        reconcile({}, prev, store, gtfs, config)
        events = reconcile(prev, curr, store, gtfs, config)
        remove_events = [e for e in events if e.event_type == "remove"]
        assert remove_events
        assert any(e.data["id"] == "v1" for e in remove_events)

    def test_unchanged_vehicle_suppressed(self) -> None:
        gtfs = _gtfs()
        config = _settings()
        store = StateStore()
        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        prev = {"v1": _event("v1", seq=5, ts=t0)}
        # same stop, only timestamp changed
        curr = {"v1": _event("v1", seq=5, ts=t1)}

        reconcile({}, prev, store, gtfs, config)
        events = reconcile(prev, curr, store, gtfs, config)
        update_events = [e for e in events if e.event_type == "update"]
        assert not update_events

    def test_stale_vehicle_emits_remove(self) -> None:
        gtfs = _gtfs()
        config = _settings(stale_vehicle_threshold_seconds=5)
        store = StateStore()
        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        # 60s > 5s threshold
        t1 = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
        prev = {"v1": _event("v1", trip_id="trip-1", ts=t0)}
        curr = {"v1": _event("v1", trip_id=None, ts=t1)}

        reconcile({}, prev, store, gtfs, config)
        events = reconcile(prev, curr, store, gtfs, config)
        remove_events = [e for e in events if e.event_type == "remove"]
        assert remove_events
