"""Integration tests for the Broadcaster pub/sub hub."""

from __future__ import annotations


from nibble.models import SSEEvent
from nibble.server import Broadcaster


def _vehicle_event(vehicle_id: str, trip_id: str = "trip-1") -> dict:
    """Return a minimal MBTA V3 vehicle resource dict for use in SSEEvents."""
    return {"id": vehicle_id, "type": "vehicle", "trip_id": trip_id}


def _sse_event(event_type: str, vehicle_id: str = "v1") -> SSEEvent:
    return SSEEvent(event_type=event_type, data=[_vehicle_event(vehicle_id)])  # type: ignore[arg-type]


class TestSubscribeUnsubscribe:
    def test_fresh_broadcaster_has_no_subscribers(self, broadcaster: Broadcaster):
        assert broadcaster.client_count == 0

    def test_subscribe_increments_count(self, broadcaster: Broadcaster):
        q = broadcaster.subscribe()
        assert broadcaster.client_count == 1
        broadcaster.unsubscribe(q)

    def test_unsubscribe_decrements_count(self, broadcaster: Broadcaster):
        q = broadcaster.subscribe()
        broadcaster.unsubscribe(q)
        assert broadcaster.client_count == 0

    def test_unsubscribe_unknown_queue_is_safe(self, broadcaster: Broadcaster):
        import asyncio

        q: asyncio.Queue = asyncio.Queue()
        broadcaster.unsubscribe(q)  # should not raise
        assert broadcaster.client_count == 0

    def test_multiple_subscribers(self, broadcaster: Broadcaster):
        q1 = broadcaster.subscribe()
        q2 = broadcaster.subscribe()
        assert broadcaster.client_count == 2
        broadcaster.unsubscribe(q1)
        broadcaster.unsubscribe(q2)


class TestCurrentResetEvent:
    def test_empty_on_fresh_broadcaster(self, broadcaster: Broadcaster):
        reset = broadcaster.current_reset_event()
        assert reset.event_type == "reset"
        assert reset.data == []

    async def test_reflects_broadcast_vehicles(self, broadcaster: Broadcaster):
        event = _sse_event("reset", "v1")
        await broadcaster.broadcast([event])
        reset = broadcaster.current_reset_event()
        assert reset.event_type == "reset"
        ids = {item["id"] for item in reset.data}
        assert "v1" in ids

    async def test_remove_event_prunes_snapshot(self, broadcaster: Broadcaster):
        # First add a vehicle via reset
        await broadcaster.broadcast([_sse_event("reset", "v1")])
        # Then remove it
        remove = SSEEvent(event_type="remove", data=[{"id": "v1"}])
        await broadcaster.broadcast([remove])
        reset = broadcaster.current_reset_event()
        ids = {item["id"] for item in reset.data}
        assert "v1" not in ids

    async def test_update_adds_to_snapshot(self, broadcaster: Broadcaster):
        await broadcaster.broadcast([_sse_event("update", "v2")])
        reset = broadcaster.current_reset_event()
        ids = {item["id"] for item in reset.data}
        assert "v2" in ids


class TestBroadcastDelivery:
    async def test_broadcast_delivers_to_subscriber(self, broadcaster: Broadcaster):
        q = broadcaster.subscribe()
        event = _sse_event("update", "v1")
        await broadcaster.broadcast([event])
        received = q.get_nowait()
        assert received.event_type == "update"
        broadcaster.unsubscribe(q)

    async def test_broadcast_delivers_to_all_subscribers(self, broadcaster: Broadcaster):
        q1 = broadcaster.subscribe()
        q2 = broadcaster.subscribe()
        event = _sse_event("update", "v1")
        await broadcaster.broadcast([event])
        r1 = q1.get_nowait()
        r2 = q2.get_nowait()
        assert r1.event_type == "update"
        assert r2.event_type == "update"
        broadcaster.unsubscribe(q1)
        broadcaster.unsubscribe(q2)

    async def test_broadcast_multiple_events_in_order(self, broadcaster: Broadcaster):
        q = broadcaster.subscribe()
        events = [_sse_event("reset", "v1"), _sse_event("update", "v2")]
        await broadcaster.broadcast(events)
        assert q.get_nowait().event_type == "reset"
        assert q.get_nowait().event_type == "update"
        broadcaster.unsubscribe(q)

    async def test_new_subscriber_gets_current_snapshot(self, broadcaster: Broadcaster):
        # Populate snapshot via broadcast before subscribing
        await broadcaster.broadcast([_sse_event("reset", "v99")])
        # New subscriber should see v99 in the reset
        reset = broadcaster.current_reset_event()
        ids = {item["id"] for item in reset.data}
        assert "v99" in ids
