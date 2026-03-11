"""Integration tests for the Starlette HTTP server: /health and /vehicles SSE endpoints."""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress

import httpx

from nibble.models import SSEEvent
from nibble.server import Broadcaster


def _parse_sse_lines(lines: list[str]) -> list[tuple[str, str]]:
    """Parse SSE text lines into (event_type, data_json) tuples."""
    events = []
    current_event = None
    current_data = None
    for line in lines:
        if line.startswith("event:"):
            current_event = line[len("event:") :].strip()
        elif line.startswith("data:"):
            current_data = line[len("data:") :].strip()
        elif line == "" and current_event is not None and current_data is not None:
            events.append((current_event, current_data))
            current_event = None
            current_data = None
    return events


async def _read_sse_events(
    async_client: httpx.AsyncClient,
    n: int,
    timeout: float = 2.0,
) -> list[tuple[str, str]]:
    """Read n SSE events from /vehicles, tolerating slow stream cleanup.

    The stream cleanup after breaking (server-side awaiting queue.get()) may be
    slow to detect the disconnect. asyncio.timeout() cancels the entire block
    after `timeout` seconds — by then `collected` is already populated from before
    the break, so the TimeoutError is suppressed and events are returned normally.
    """
    collected: list[tuple[str, str]] = []

    async def _do_stream() -> None:
        lines: list[str] = []
        async with async_client.stream("GET", "/vehicles") as response:
            async for line in response.aiter_lines():
                lines.append(line)
                parsed = _parse_sse_lines(lines)
                if len(parsed) >= n:
                    collected.extend(parsed[:n])
                    return  # events captured; __aexit__ cleanup follows

    with suppress(TimeoutError):
        async with asyncio.timeout(timeout):
            await _do_stream()

    return collected


class TestHealthEndpoint:
    async def test_health_returns_200(self, async_client: httpx.AsyncClient):
        response = await async_client.get("/health")
        assert response.status_code == 200

    async def test_health_response_shape(self, async_client: httpx.AsyncClient):
        response = await async_client.get("/health")
        body = response.json()
        assert body["status"] == "ok"
        assert "last_poll_time" in body
        assert "connected_clients" in body

    async def test_health_initial_last_poll_time_is_null(self, async_client: httpx.AsyncClient):
        response = await async_client.get("/health")
        assert response.json()["last_poll_time"] is None

    async def test_health_initial_client_count_is_zero(self, async_client: httpx.AsyncClient):
        response = await async_client.get("/health")
        assert response.json()["connected_clients"] == 0


class TestVehiclesSSEEndpoint:
    async def test_vehicles_sends_initial_reset_event(self, async_client: httpx.AsyncClient):
        """New SSE client receives an immediate reset event (even with empty state)."""
        events = await _read_sse_events(async_client, n=1)
        assert events, "Expected at least one SSE event from /vehicles"
        assert events[0][0] == "reset"

    async def test_vehicles_reset_data_is_valid_json(self, async_client: httpx.AsyncClient):
        events = await _read_sse_events(async_client, n=1)
        assert events
        data = json.loads(events[0][1])
        assert isinstance(data, list)

    async def test_vehicles_reset_contains_prepopulated_vehicles(
        self, broadcaster: Broadcaster, async_client: httpx.AsyncClient
    ):
        """Vehicles broadcast before client connects appear in the initial reset."""
        sse_event = SSEEvent(
            event_type="reset",
            data=[{"id": "v-prepopulated", "type": "vehicle"}],  # type: ignore[list-item]
        )
        await broadcaster.broadcast([sse_event])

        events = await _read_sse_events(async_client, n=1)
        assert events
        data = json.loads(events[0][1])
        ids = [item["id"] for item in data]
        assert "v-prepopulated" in ids

    async def test_vehicles_streams_broadcast_update(
        self, broadcaster: Broadcaster, async_client: httpx.AsyncClient
    ):
        """After the initial reset, a broadcast update is received by the SSE client."""
        update = SSEEvent(
            event_type="update",
            data=[{"id": "v-new", "type": "vehicle"}],  # type: ignore[list-item]
        )

        async def _inject_update() -> None:
            await asyncio.sleep(0.05)  # let the SSE connection establish
            await broadcaster.broadcast([update])

        inject_task = asyncio.create_task(_inject_update())
        events = await _read_sse_events(async_client, n=2, timeout=3.0)
        await inject_task

        event_types = [e[0] for e in events]
        assert "reset" in event_types
        assert "update" in event_types

    async def test_health_reflects_connected_client(
        self, broadcaster: Broadcaster, async_client: httpx.AsyncClient
    ):
        """A connected SSE client should increment the connected_clients count."""
        ready = asyncio.Event()

        async def _hold_connection() -> None:
            lines: list[str] = []
            with suppress(TimeoutError):
                async with asyncio.timeout(1.5):
                    async with async_client.stream("GET", "/vehicles") as response:
                        async for line in response.aiter_lines():
                            lines.append(line)
                            if _parse_sse_lines(lines):
                                ready.set()
                                await asyncio.sleep(0.3)  # stay connected
                                return

        task = asyncio.create_task(_hold_connection())
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        # Query /health while the first client is still connected
        transport = httpx.ASGITransport(app=async_client._transport._app)  # type: ignore[attr-defined]
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client2:
            response = await client2.get("/health")
            count = response.json()["connected_clients"]

        with suppress(asyncio.CancelledError, TimeoutError):
            await task
        assert count >= 1
