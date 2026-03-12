"""Integration tests for the Broadcaster pub/sub hub."""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from nibble.models import SSEEvent
from nibble.server import Broadcaster


def _vehicle_event(vehicle_id: str, trip_id: str = "trip-1") -> dict[str, Any]:
    """Return a minimal MBTA V3 vehicle resource dict for use in SSEEvents."""
    return {"id": vehicle_id, "type": "vehicle", "trip_id": trip_id}


def _sse_event(
    event_type: Literal["reset", "update", "remove"], vehicle_id: str = "v1"
) -> SSEEvent:
    return SSEEvent(event_type=event_type, data=[_vehicle_event(vehicle_id)])


class TestSubscribeUnsubscribe:
    def test_fresh_broadcaster_has_no_subscribers(self, broadcaster: Broadcaster) -> None:
        assert broadcaster.client_count == 0

    def test_subscribe_increments_count(self, broadcaster: Broadcaster) -> None:
        q = broadcaster.subscribe()
        assert broadcaster.client_count == 1
        broadcaster.unsubscribe(q)

    def test_unsubscribe_decrements_count(self, broadcaster: Broadcaster) -> None:
        q = broadcaster.subscribe()
        broadcaster.unsubscribe(q)
        assert broadcaster.client_count == 0

    def test_unsubscribe_unknown_queue_is_safe(self, broadcaster: Broadcaster) -> None:
        q: asyncio.Queue[SSEEvent | None] = asyncio.Queue()
        broadcaster.unsubscribe(q)  # should not raise
        assert broadcaster.client_count == 0

    def test_multiple_subscribers(self, broadcaster: Broadcaster) -> None:
        q1 = broadcaster.subscribe()
        q2 = broadcaster.subscribe()
        assert broadcaster.client_count == 2
        broadcaster.unsubscribe(q1)
        broadcaster.unsubscribe(q2)


class TestCurrentResetEvent:
    def test_empty_on_fresh_broadcaster(self, broadcaster: Broadcaster) -> None:
        reset = broadcaster.current_reset_event()
        assert reset.event_type == "reset"
        assert reset.data == []

    async def test_reflects_broadcast_vehicles(self, broadcaster: Broadcaster) -> None:
        event = _sse_event("reset", "v1")
        await broadcaster.broadcast([event])
        reset = broadcaster.current_reset_event()
        assert reset.event_type == "reset"
        ids = {item["id"] for item in reset.data}
        assert "v1" in ids

    async def test_remove_event_prunes_snapshot(self, broadcaster: Broadcaster) -> None:
        # First add a vehicle via reset
        await broadcaster.broadcast([_sse_event("reset", "v1")])
        # Then remove it
        remove = SSEEvent(event_type="remove", data=[{"id": "v1"}])
        await broadcaster.broadcast([remove])
        reset = broadcaster.current_reset_event()
        ids = {item["id"] for item in reset.data}
        assert "v1" not in ids

    async def test_update_adds_to_snapshot(self, broadcaster: Broadcaster) -> None:
        await broadcaster.broadcast([_sse_event("update", "v2")])
        reset = broadcaster.current_reset_event()
        ids = {item["id"] for item in reset.data}
        assert "v2" in ids


class TestBroadcastDelivery:
    async def test_broadcast_delivers_to_subscriber(self, broadcaster: Broadcaster) -> None:
        q = broadcaster.subscribe()
        event = _sse_event("update", "v1")
        await broadcaster.broadcast([event])
        received = q.get_nowait()
        assert received is not None
        assert received.event_type == "update"
        broadcaster.unsubscribe(q)

    async def test_broadcast_delivers_to_all_subscribers(self, broadcaster: Broadcaster) -> None:
        q1 = broadcaster.subscribe()
        q2 = broadcaster.subscribe()
        event = _sse_event("update", "v1")
        await broadcaster.broadcast([event])
        r1 = q1.get_nowait()
        r2 = q2.get_nowait()
        assert r1 is not None and r2 is not None
        assert r1.event_type == "update"
        assert r2.event_type == "update"
        broadcaster.unsubscribe(q1)
        broadcaster.unsubscribe(q2)

    async def test_broadcast_multiple_events_in_order(self, broadcaster: Broadcaster) -> None:
        q = broadcaster.subscribe()
        events = [_sse_event("reset", "v1"), _sse_event("update", "v2")]
        await broadcaster.broadcast(events)
        first = q.get_nowait()
        second = q.get_nowait()
        assert first is not None and second is not None
        assert first.event_type == "reset"
        assert second.event_type == "update"
        broadcaster.unsubscribe(q)

    async def test_new_subscriber_gets_current_snapshot(self, broadcaster: Broadcaster) -> None:
        # Populate snapshot via broadcast before subscribing
        await broadcaster.broadcast([_sse_event("reset", "v99")])
        # New subscriber should see v99 in the reset
        reset = broadcaster.current_reset_event()
        ids = {item["id"] for item in reset.data}
        assert "v99" in ids
