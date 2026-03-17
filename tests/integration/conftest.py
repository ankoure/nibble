"""Shared fixtures for nibble integration tests."""

from __future__ import annotations

import asyncio
import io
import zipfile
from collections.abc import AsyncGenerator, AsyncIterator, MutableMapping
from contextlib import suppress
from typing import Any, cast

import pytest
import pytest_asyncio
import httpx
from fastapi import FastAPI
from google.transit import gtfs_realtime_pb2
from starlette.types import ASGIApp

from nibble.config import Settings
from nibble.gtfs.static import StaticGTFS, _parse_gtfs_zip
from nibble.overrides import OverrideStore
from nibble.server import Broadcaster, GtfsHolder, create_app


# ---------------------------------------------------------------------------
# Streaming ASGI transport (required for SSE)
# ---------------------------------------------------------------------------
# httpx's built-in ASGITransport buffers the entire response before returning,
# so it cannot stream SSE.  This transport runs the ASGI app in a background
# asyncio task and pipes response chunks through a queue so aiter_lines() works.


class _StreamingBody(httpx.AsyncByteStream):
    def __init__(
        self,
        queue: asyncio.Queue[bytes | None],
        app_task: asyncio.Task[None],
        disconnect: asyncio.Event,
    ) -> None:
        self._queue = queue
        self._app_task = app_task
        self._disconnect = disconnect

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await self._queue.get()
            if chunk is None:
                break
            yield chunk

    async def aclose(self) -> None:
        self._disconnect.set()
        self._app_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await self._app_task


class StreamingASGITransport(httpx.AsyncBaseTransport):
    """ASGI transport that delivers chunks incrementally — required for SSE tests."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        query = request.url.query
        if isinstance(query, str):
            query = query.encode()

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "headers": [(k.lower(), v) for k, v in request.headers.raw],
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": query,
            "server": (request.url.host, request.url.port or 80),
            "client": ("127.0.0.1", 123),
            "root_path": "",
        }

        chunks = cast(httpx.AsyncByteStream, request.stream).__aiter__()
        request_complete = False
        disconnect = asyncio.Event()

        async def receive() -> dict[str, Any]:
            nonlocal request_complete
            if request_complete:
                await disconnect.wait()
                return {"type": "http.disconnect"}
            try:
                body = await chunks.__anext__()
                return {"type": "http.request", "body": body, "more_body": True}
            except StopAsyncIteration:
                request_complete = True
                return {"type": "http.request", "body": b"", "more_body": False}

        status_code: int | None = None
        resp_headers: list[tuple[bytes, bytes]] | None = None
        body_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        response_started: asyncio.Event = asyncio.Event()

        async def send(message: MutableMapping[str, Any]) -> None:
            nonlocal status_code, resp_headers
            if message["type"] == "http.response.start":
                status_code = message["status"]
                resp_headers = message.get("headers", [])
                response_started.set()
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                more_body = message.get("more_body", False)
                if body:
                    await body_queue.put(body)
                if not more_body:
                    await body_queue.put(None)

        app_task: asyncio.Task[None] = asyncio.create_task(self._app(scope, receive, send))  # type: ignore[arg-type]
        await response_started.wait()

        assert status_code is not None
        stream = _StreamingBody(body_queue, app_task, disconnect)
        return httpx.Response(status_code, headers=resp_headers or [], stream=stream)


# ---------------------------------------------------------------------------
# Static GTFS fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def gtfs_zip_bytes() -> bytes:
    """In-memory GTFS ZIP with 2 trips (trip-1, trip-2) and 3 stops each."""
    trips_csv = (
        "route_id,service_id,trip_id,direction_id\nroute-1,svc-1,trip-1,0\nroute-1,svc-1,trip-2,1\n"
    )
    stop_times_csv = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "trip-1,08:00:00,08:00:00,stop-A,1\n"
        "trip-1,08:05:00,08:05:00,stop-B,2\n"
        "trip-1,08:10:00,08:10:00,stop-C,3\n"
        "trip-2,09:00:00,09:00:00,stop-A,1\n"
        "trip-2,09:05:00,09:05:00,stop-B,2\n"
        "trip-2,09:10:00,09:10:00,stop-C,3\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)
    return buf.getvalue()


@pytest.fixture
def static_gtfs(gtfs_zip_bytes: bytes) -> StaticGTFS:
    """Real StaticGTFS loaded from the in-memory ZIP (no HTTP)."""
    return _parse_gtfs_zip(gtfs_zip_bytes)


# ---------------------------------------------------------------------------
# GTFS-RT fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def feed_message() -> gtfs_realtime_pb2.FeedMessage:
    """Real FeedMessage protobuf with 2 vehicles: v1 on trip-1 (seq 1), v2 on trip-2 (seq 1)."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1704067200  # 2024-01-01 12:00:00 UTC

    e1 = feed.entity.add()
    e1.id = "e1"
    e1.vehicle.vehicle.id = "v1"
    e1.vehicle.trip.trip_id = "trip-1"
    e1.vehicle.trip.route_id = "route-1"
    e1.vehicle.position.latitude = 41.82
    e1.vehicle.position.longitude = -71.41
    e1.vehicle.current_stop_sequence = 1
    e1.vehicle.timestamp = 1704067200

    e2 = feed.entity.add()
    e2.id = "e2"
    e2.vehicle.vehicle.id = "v2"
    e2.vehicle.trip.trip_id = "trip-2"
    e2.vehicle.trip.route_id = "route-1"
    e2.vehicle.position.latitude = 41.83
    e2.vehicle.position.longitude = -71.42
    e2.vehicle.current_stop_sequence = 1
    e2.vehicle.timestamp = 1704067200

    return feed


# ---------------------------------------------------------------------------
# Server fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        gtfs_rt_url="http://example.com/rt",
        gtfs_static_url="http://example.com/static.zip",
    )


@pytest.fixture
def broadcaster() -> Broadcaster:
    return Broadcaster()


@pytest.fixture
def app(
    settings: Settings, broadcaster: Broadcaster, static_gtfs: StaticGTFS, tmp_path: Any
) -> FastAPI:
    overrides = OverrideStore(tmp_path / "overrides.json")
    return create_app(settings, broadcaster, overrides, GtfsHolder(static_gtfs))


@pytest_asyncio.fixture
async def async_client(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Async HTTPX client backed by the FastAPI ASGI app (streaming transport for SSE)."""
    transport = StreamingASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
