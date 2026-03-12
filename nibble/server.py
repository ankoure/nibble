"""Starlette ASGI server: SSE endpoint, health check, and startup orchestration."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from collections.abc import MutableMapping
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import uvicorn
from sse_starlette.sse import EventSourceResponse
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from nibble.adapters import get_adapter
from nibble.config import Settings
from nibble.gtfs.feed_info import parse_feed_info
from nibble.gtfs.fixer import fix_gtfs_zip
from nibble.gtfs.static import StaticGTFS, load_static_gtfs, load_static_gtfs_from_bytes
from nibble.models import SSEEvent
from nibble.poller import poll_loop

logger = logging.getLogger(__name__)

_LOG_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)


class JsonFormatter(logging.Formatter):
    """Serialize log records to JSON lines for structured log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        ts = (
            datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3]
            + "Z"
        )
        obj: dict[str, Any] = {
            "timestamp": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _LOG_RESERVED:
                obj[key] = value
        if record.exc_info:
            obj["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(obj)


def configure_logging(config: Settings) -> None:
    """Configure the root logger based on application settings.

    Args:
        config: Application settings providing ``log_level`` and ``log_json``.
    """
    level = getattr(logging, config.log_level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    if config.log_json:
        handler.setFormatter(JsonFormatter())
        # Suppress uvicorn's built-in access log to avoid duplicate request lines.
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)


class LoggingMiddleware:
    """ASGI middleware that logs each HTTP request with method, path, status, and duration."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.monotonic()
        status_code = 0

        async def send_wrapper(message: MutableMapping[str, Any]) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        await self.app(scope, receive, send_wrapper)

        duration_ms = round((time.monotonic() - start) * 1000)
        method = scope.get("method", "")
        path = scope.get("path", "")
        query = scope.get("query_string", b"").decode()
        full_path = f"{path}?{query}" if query else path
        logger.info(
            "HTTP %s %s",
            method,
            full_path,
            extra={"status_code": status_code, "duration_ms": duration_ms},
        )


class Broadcaster:
    """Async pub/sub hub for SSE clients.

    Maintains a current snapshot of vehicle state so that new subscribers
    receive an immediate ``reset`` event without waiting for the next poll.
    Each subscriber gets its own ``asyncio.Queue``; the poll loop pushes
    ``SSEEvent`` objects into every queue via ``broadcast()``.
    """

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[SSEEvent | None]] = set()
        self.last_poll_time: datetime | None = None
        self._current_snapshot: dict[str, dict[str, Any]] = {}

    def subscribe(self) -> asyncio.Queue[SSEEvent | None]:
        """Register a new SSE client and return its dedicated event queue.

        Returns:
            A new ``asyncio.Queue`` that will receive ``SSEEvent`` objects as
            they are broadcast. A ``None`` sentinel is enqueued to signal
            stream termination.
        """
        q: asyncio.Queue[SSEEvent | None] = asyncio.Queue()
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[SSEEvent | None]) -> None:
        """Remove a client's queue (called when the client disconnects).

        Args:
            q: The queue returned by :meth:`subscribe` when this client connected.
        """
        self._subscribers.discard(q)

    async def broadcast(self, events: list[SSEEvent]) -> None:
        """Push events to all subscriber queues and update the current snapshot.

        Args:
            events: The ``SSEEvent`` objects to enqueue for every connected client.
                ``"reset"`` and ``"update"`` events update the internal vehicle
                snapshot; ``"remove"`` events remove vehicles from it.
        """
        for event in events:
            if event.event_type in ("reset", "update"):
                for item in event.data:
                    if "id" in item:
                        self._current_snapshot[item["id"]] = item
            elif event.event_type == "remove":
                for item in event.data:
                    self._current_snapshot.pop(item.get("id", ""), None)

        for q in list(self._subscribers):
            for event in events:
                await q.put(event)

    def current_reset_event(self) -> SSEEvent:
        """Build a reset SSEEvent from the current vehicle snapshot for new subscribers.

        Returns:
            An ``SSEEvent`` with ``event_type="reset"`` containing all currently
            tracked vehicles. Sent immediately to each new client on connection.
        """
        return SSEEvent(
            event_type="reset",
            data=list(self._current_snapshot.values()),
        )

    @property
    def client_count(self) -> int:
        """Number of currently connected SSE clients."""
        return len(self._subscribers)


def create_app(config: Settings, broadcaster: Broadcaster) -> Starlette:
    """Build and return the Starlette ASGI application with /vehicles and /health routes.

    Args:
        config: Application settings (unused directly, reserved for future route config).
        broadcaster: The pub/sub hub that SSE clients subscribe to and the poll
            loop broadcasts into.

    Returns:
        A configured ``Starlette`` application with two routes:

        - ``GET /vehicles`` — SSE stream of vehicle events
        - ``GET /health`` — JSON health check
    """

    async def vehicles(request: Request) -> EventSourceResponse:
        route_filter = request.query_params.get("filter[route]")
        q = broadcaster.subscribe()

        reset = broadcaster.current_reset_event()

        def matches_route(item: dict[str, Any]) -> bool:
            if not route_filter:
                return True
            route_data = item.get("relationships", {}).get("route", {}).get("data") or {}
            return route_data.get("id") == route_filter

        async def stream() -> AsyncIterator[dict[str, Any]]:
            known_ids: set[str] = set()
            try:
                filtered = [v for v in reset.data if matches_route(v)]
                known_ids = {v["id"] for v in filtered}
                yield {"event": "reset", "data": json.dumps(filtered)}
                while True:
                    event = await q.get()
                    if event is None:
                        break
                    if event.event_type == "remove":
                        items = [v for v in event.data if v.get("id") in known_ids]
                        known_ids -= {v["id"] for v in items}
                    else:
                        items = [v for v in event.data if matches_route(v)]
                        known_ids |= {v["id"] for v in items}
                    if items:
                        yield {"event": event.event_type, "data": json.dumps(items)}
            finally:
                broadcaster.unsubscribe(q)

        return EventSourceResponse(stream())

    async def health(request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "status": "ok",
                "last_poll_time": broadcaster.last_poll_time.isoformat()
                if broadcaster.last_poll_time
                else None,
                "connected_clients": broadcaster.client_count,
            }
        )

    return Starlette(
        routes=[
            Route("/vehicles", vehicles),
            Route("/health", health),
        ],
        middleware=[Middleware(LoggingMiddleware)],
    )


def _load_gtfs(config: Settings) -> StaticGTFS:
    """Download static GTFS, optionally fixing and publishing to S3 first.

    When ``config.gtfs_static_fix`` is ``True``, the raw ZIP is downloaded,
    cleaned by :func:`nibble.gtfs.fixer.fix_gtfs_zip`, published to S3 via
    :func:`nibble.gtfs.publisher.publish_gtfs_to_s3`, then parsed from the
    corrected bytes. Otherwise the ZIP is fetched and parsed directly.

    Args:
        config: Application settings providing URLs, S3 credentials, and the
            ``gtfs_static_fix`` flag.

    Returns:
        A ``StaticGTFS`` object with parsed trip and stop-time indexes.

    Raises:
        ValueError: If ``gtfs_static_fix`` is ``True`` but ``s3_bucket`` is unset.
        httpx.HTTPStatusError: If the static GTFS download fails.
    """

    if config.gtfs_static_fix:
        if not config.s3_bucket:
            raise ValueError("NIBBLE_S3_BUCKET must be set when NIBBLE_GTFS_STATIC_FIX=true")

        import httpx

        from nibble.gtfs.publisher import publish_gtfs_to_s3

        logger.info("Downloading raw static GTFS from %s for fixing", config.gtfs_static_url)
        response = httpx.get(config.gtfs_static_url, follow_redirects=True, timeout=60)
        response.raise_for_status()
        raw_zip = response.content

        logger.info("Applying GTFS fixes")
        fixed_zip = fix_gtfs_zip(raw_zip)

        feed_info = parse_feed_info(fixed_zip)
        if feed_info is None:
            logger.warning("feed_info.txt not found in GTFS ZIP; using today as feed_start_date")
            from datetime import date

            today = date.today().strftime("%Y%m%d")
            from nibble.gtfs.feed_info import FeedInfo

            feed_info = FeedInfo(feed_start_date=today, feed_end_date=today, feed_version="unknown")

        publish_gtfs_to_s3(
            zip_bytes=fixed_zip,
            feed_info=feed_info,
            bucket=config.s3_bucket,
            prefix=config.s3_prefix,
            archived_feeds_key=config.s3_archived_feeds_key,
            region=config.s3_region,
        )

        return load_static_gtfs_from_bytes(fixed_zip)

    return load_static_gtfs(config.gtfs_static_url)


def main() -> None:
    """Entry point: load config, download static GTFS, start server and poll loop."""
    config = Settings()  # type: ignore[call-arg]
    configure_logging(config)
    gtfs = _load_gtfs(config)
    adapter = get_adapter(config.adapter, config.gtfs_rt_url, config.agency_id)

    broadcaster = Broadcaster()
    app = create_app(config, broadcaster)

    async def startup() -> None:
        asyncio.create_task(poll_loop(config, gtfs, broadcaster, adapter))

    app.add_event_handler("startup", startup)

    uvicorn.run(app, host=config.host, port=config.port)


if __name__ == "__main__":
    main()
