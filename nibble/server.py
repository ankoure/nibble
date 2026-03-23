"""FastAPI ASGI server: SSE endpoint, health check, and startup orchestration."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import sys
import time
import zipfile
from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any

import uvicorn
from fastapi import FastAPI, Query
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from nibble.adapters import get_adapter
from nibble.adapters.base import BaseAdapter
from nibble.config import Settings
from nibble.gtfs.feed_info import FeedInfo, dates_from_calendar, parse_feed_info
from nibble.gtfs.fixer import fix_gtfs_zip
from nibble.gtfs.static import (
    StaticGTFS,
    last_stop_sequence,
    load_static_gtfs,
    load_static_gtfs_from_bytes,
)
from nibble.headways import compute_headways
from nibble.models import SSEEvent, VehicleEvent
from nibble.overrides import OverrideStore
from nibble.poller import poll_loop
from nibble.predictions import compute_delay, predict_arrivals


class GtfsHolder:
    """Container for the current ``StaticGTFS`` instance, safe for asyncio use.

    The reload loop swaps :attr:`gtfs` in place; the poll loop always reads
    the latest value without needing a lock.  This is safe because both loops
    run in the same asyncio event loop — there are no ``await`` points between
    reading and using ``holder.gtfs``, so no other coroutine can interleave.
    """

    def __init__(self, gtfs: StaticGTFS) -> None:
        self.gtfs = gtfs


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
        self.vehicle_snapshot: dict[str, VehicleEvent] = {}

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
            if event.event_type == "reset":
                for item in event.data:
                    if isinstance(item, dict) and "id" in item:
                        self._current_snapshot[item["id"]] = item
            elif event.event_type == "update":
                if isinstance(event.data, dict) and "id" in event.data:
                    self._current_snapshot[event.data["id"]] = event.data
            elif event.event_type == "remove":
                if isinstance(event.data, dict):
                    self._current_snapshot.pop(event.data.get("id", ""), None)

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


class TripAssignmentRequest(BaseModel):
    vehicle_id: str
    trip_id: str


class TripAssignmentResponse(BaseModel):
    vehicle_id: str
    trip_id: str
    assigned_at: str


class AssignmentDetail(BaseModel):
    trip_id: str
    assigned_at: str


class StopPrediction(BaseModel):
    stop_id: str
    stop_sequence: int
    scheduled_arrival: str
    predicted_arrival: str
    delay_seconds: int


class TripPredictionsResponse(BaseModel):
    trip_id: str
    vehicle_id: str
    delay_seconds: int | None
    stop_predictions: list[StopPrediction]


class HealthResponse(BaseModel):
    status: str
    last_poll_time: str | None
    connected_clients: int


class ErrorResponse(BaseModel):
    error: str


def create_app(
    config: Settings,
    broadcaster: Broadcaster,
    overrides: OverrideStore,
    gtfs_holder: GtfsHolder,
    adapter: BaseAdapter | None = None,
    on_snapshot: Callable[[dict[str, VehicleEvent]], Awaitable[None]] | None = None,
) -> FastAPI:
    """Build and return the FastAPI ASGI application.

    When *adapter* is provided, the poll loop and optional GTFS reload loop are
    started as background tasks during the FastAPI lifespan and cancelled cleanly
    on shutdown.  When *adapter* is ``None`` (e.g. for OpenAPI schema generation),
    no background tasks are started.

    Args:
        config: Application settings.
        broadcaster: The pub/sub hub that SSE clients subscribe to and the poll
            loop broadcasts into.
        overrides: Store for operator-issued manual trip assignment corrections.
        gtfs_holder: Shared container for the current static GTFS indexes, used
            to validate trip IDs submitted via the corrections API.
        adapter: Feed adapter to use. When supplied, the poll loop is started
            as part of the application lifespan.
        on_snapshot: Optional async callback invoked after each successful poll.

    Returns:
        A configured ``FastAPI`` application with the following routes:

        - ``GET /vehicles`` - SSE stream of vehicle events
        - ``GET /health`` - JSON health check
        - ``POST /trip_assignments`` - create a manual trip assignment
        - ``GET /trip_assignments`` - list active manual trip assignments
        - ``DELETE /trip_assignments/{vehicle_id}`` - remove a manual assignment
        - ``GET /trips/{trip_id}/predictions`` - arrival predictions for a trip
        - ``GET /routes/{route_id}/headways`` - headway metrics for a route
    """

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        tasks: list[asyncio.Task[None]] = []
        if adapter is not None:
            tasks.append(
                asyncio.create_task(
                    poll_loop(config, gtfs_holder, broadcaster, adapter, overrides, on_snapshot)
                )
            )
        if config.gtfs_reload_interval_hours is not None:
            tasks.append(asyncio.create_task(gtfs_reload_loop(config, gtfs_holder)))
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    app = FastAPI(title="nibble", version="0.1.0", lifespan=_lifespan)
    app.add_middleware(LoggingMiddleware)

    @app.get(
        "/vehicles",
        response_class=EventSourceResponse,
        responses={200: {"description": "SSE stream of vehicle events (reset/add/update/remove)"}},
    )
    async def vehicles(
        request: Request,
        filter_route: str | None = Query(default=None, alias="filter[route]"),
    ) -> EventSourceResponse:
        if filter_route is None:
            route_filter_set = None  # param absent → no filter, return all
        elif filter_route:
            route_filter_set = set(filter_route.split(","))
        else:
            route_filter_set = set()  # param present but empty → match nothing
        q = broadcaster.subscribe()

        reset = broadcaster.current_reset_event()

        def matches_route(item: dict[str, Any]) -> bool:
            if route_filter_set is None:
                return True
            route_data = item.get("relationships", {}).get("route", {}).get("data") or {}
            return route_data.get("id") in route_filter_set

        async def stream() -> AsyncIterator[dict[str, Any]]:
            known_ids: set[str] = set()
            try:
                filtered = [v for v in reset.data if isinstance(v, dict) and matches_route(v)]
                known_ids = {v["id"] for v in filtered}
                yield {"event": "reset", "data": json.dumps(filtered)}
                while True:
                    event = await q.get()
                    if event is None:
                        break
                    if event.event_type == "reset":
                        if not isinstance(event.data, list):
                            continue
                        filtered = [
                            v for v in event.data if isinstance(v, dict) and matches_route(v)
                        ]
                        known_ids = {v["id"] for v in filtered}
                        yield {"event": "reset", "data": json.dumps(filtered)}
                    elif event.event_type == "remove":
                        if not isinstance(event.data, dict):
                            continue
                        if event.data.get("id") in known_ids:
                            known_ids.discard(event.data["id"])
                            yield {"event": event.event_type, "data": json.dumps(event.data)}
                    else:
                        if not isinstance(event.data, dict):
                            continue
                        if matches_route(event.data):
                            known_ids.add(event.data["id"])
                            yield {"event": event.event_type, "data": json.dumps(event.data)}
            finally:
                broadcaster.unsubscribe(q)

        return EventSourceResponse(stream())

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            last_poll_time=broadcaster.last_poll_time.isoformat()
            if broadcaster.last_poll_time
            else None,
            connected_clients=broadcaster.client_count,
        )

    @app.post(
        "/trip_assignments",
        response_model=TripAssignmentResponse,
        responses={422: {"model": ErrorResponse}},
    )
    async def post_trip_assignment(
        body: TripAssignmentRequest,
    ) -> TripAssignmentResponse | JSONResponse:
        vehicle_id = body.vehicle_id
        trip_id = body.trip_id

        if last_stop_sequence(gtfs_holder.gtfs, trip_id) is None:
            return JSONResponse(
                {"error": f"trip_id {trip_id!r} not found in static GTFS"},
                status_code=422,
            )

        assigned_at = overrides.set(vehicle_id, trip_id)
        logger.info("Manual trip assignment: vehicle=%s trip=%s", vehicle_id, trip_id)
        return TripAssignmentResponse(
            vehicle_id=vehicle_id, trip_id=trip_id, assigned_at=assigned_at
        )

    @app.get("/trip_assignments", response_model=dict[str, AssignmentDetail])
    async def get_trip_assignments() -> dict[str, dict[str, str]]:
        return overrides.all()

    @app.delete("/trip_assignments/{vehicle_id}", status_code=204)
    async def delete_trip_assignment(vehicle_id: str) -> None:
        overrides.remove(vehicle_id)

    @app.get(
        "/trips/{trip_id}/predictions",
        response_model=TripPredictionsResponse,
        responses={404: {"model": ErrorResponse}},
    )
    async def trip_predictions(trip_id: str) -> TripPredictionsResponse | JSONResponse:
        vehicle = next(
            (e for e in broadcaster.vehicle_snapshot.values() if e.trip_id == trip_id),
            None,
        )
        if vehicle is None:
            return JSONResponse(
                {"error": f"no active vehicle on trip {trip_id!r}"},
                status_code=404,
            )
        delay = compute_delay(vehicle, gtfs_holder.gtfs, config.agency_timezone)
        stop_predictions = predict_arrivals(vehicle, gtfs_holder.gtfs, config.agency_timezone)
        return TripPredictionsResponse(
            trip_id=trip_id,
            vehicle_id=vehicle.vehicle_id,
            delay_seconds=delay,
            stop_predictions=stop_predictions,
        )

    @app.get("/routes/{route_id}/headways")
    async def route_headways(route_id: str) -> JSONResponse:
        result = compute_headways(route_id, broadcaster.vehicle_snapshot, gtfs_holder.gtfs)
        return JSONResponse(result)

    @app.get("/unknown_routes")
    async def get_unknown_routes() -> JSONResponse:
        from nibble import unknown_routes

        return JSONResponse(unknown_routes.all_entries())

    @app.delete("/unknown_routes", status_code=204)
    async def delete_unknown_routes() -> None:
        from nibble import unknown_routes

        unknown_routes.clear()

    @app.get(
        "/archived_feeds",
        response_model=None,
        responses={
            200: {"content": {"text/csv": {}}, "description": "archived_feeds.txt CSV"},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def get_archived_feeds(request: Request) -> Response | JSONResponse:
        if not config.s3_bucket:
            return JSONResponse(
                {"error": "S3 is not configured (NIBBLE_S3_BUCKET is unset)"},
                status_code=503,
            )

        try:
            import boto3
        except ImportError:
            return JSONResponse(
                {"error": "boto3 is not installed; install nibble[s3]"},
                status_code=503,
            )

        slug = config.s3_agency_slug
        key = f"{slug}/{config.s3_archived_feeds_key}" if slug else config.s3_archived_feeds_key

        def _fetch() -> bytes:
            s3 = boto3.client("s3", region_name=config.s3_region)
            return s3.get_object(Bucket=config.s3_bucket, Key=key)["Body"].read()

        try:
            content = await asyncio.to_thread(_fetch)
        except Exception as exc:
            if "NoSuchKey" in str(exc) or "404" in str(exc):
                return JSONResponse(
                    {"error": "archived_feeds.txt not found in S3"}, status_code=404
                )
            logger.exception("Failed to read archived_feeds from S3")
            return JSONResponse({"error": "failed to read archived feeds from S3"}, status_code=503)

        base_url = str(request.base_url).rstrip("/")
        reader = csv.DictReader(io.StringIO(content.decode()))
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=reader.fieldnames or [], lineterminator="\n")
        writer.writeheader()
        for row in reader:
            url = row.get("archive_url", "")
            if url.startswith("/"):
                row["archive_url"] = f"{base_url}{url}"
            writer.writerow(row)

        return Response(content=buf.getvalue(), media_type="text/csv")

    @app.get(
        "/gtfs/{filename}",
        response_model=None,
        responses={
            200: {"content": {"application/zip": {}}, "description": "GTFS ZIP archive"},
            400: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def get_gtfs_file(filename: str) -> Response | JSONResponse:
        import re

        if not re.fullmatch(r"[0-9A-Za-z_-]+\.zip", filename):
            return JSONResponse({"error": "invalid filename"}, status_code=400)

        if not config.s3_bucket:
            return JSONResponse(
                {"error": "S3 is not configured (NIBBLE_S3_BUCKET is unset)"},
                status_code=503,
            )

        try:
            import boto3
        except ImportError:
            return JSONResponse(
                {"error": "boto3 is not installed; install nibble[s3]"},
                status_code=503,
            )

        slug = config.s3_agency_slug
        key = f"{slug}/{config.s3_prefix}/{filename}" if slug else f"{config.s3_prefix}/{filename}"

        def _fetch() -> bytes:
            s3 = boto3.client("s3", region_name=config.s3_region)
            return s3.get_object(Bucket=config.s3_bucket, Key=key)["Body"].read()

        try:
            content = await asyncio.to_thread(_fetch)
        except Exception as exc:
            if "NoSuchKey" in str(exc) or "404" in str(exc):
                return JSONResponse({"error": f"{filename} not found"}, status_code=404)
            logger.exception("Failed to fetch %s from S3", filename)
            return JSONResponse({"error": "failed to fetch file from S3"}, status_code=503)

        return Response(
            content=content,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return app


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
            logger.warning(
                "feed_info.txt not found in GTFS ZIP; deriving dates from calendar files"
            )

            today = date.today().strftime("%Y%m%d")
            with zipfile.ZipFile(io.BytesIO(fixed_zip)) as zf:
                start_date, end_date = dates_from_calendar(zf)
            feed_info = FeedInfo(
                feed_start_date=start_date or today,
                feed_end_date=end_date,
                feed_version="unknown",
            )

        slug = config.s3_agency_slug
        publish_gtfs_to_s3(
            zip_bytes=fixed_zip,
            feed_info=feed_info,
            bucket=config.s3_bucket,
            prefix=f"{slug}/{config.s3_prefix}" if slug else config.s3_prefix,
            archived_feeds_key=f"{slug}/{config.s3_archived_feeds_key}"
            if slug
            else config.s3_archived_feeds_key,
            region=config.s3_region,
            archive_url_base="/gtfs",
        )

        return load_static_gtfs_from_bytes(fixed_zip)

    return load_static_gtfs(config.gtfs_static_url)


async def gtfs_reload_loop(config: Settings, holder: GtfsHolder) -> None:
    """Periodically re-download the static GTFS and reload if the feed has changed.

    Uses ``feed_start_date`` from ``feed_info.txt`` as the change fingerprint
    when available, falling back to an MD5 hash of the ZIP for feeds that omit
    ``feed_info.txt``. If the fingerprint differs from the last-seen value, the
    fixed ZIP is published to S3 (when ``gtfs_static_fix`` is enabled) and the
    holder's ``gtfs`` reference is swapped to the new indexes.

    Args:
        config: Application settings. ``gtfs_reload_interval_hours`` controls
            the sleep interval between checks.
        holder: Shared container whose ``gtfs`` attribute is replaced on reload.
    """
    interval_seconds = (config.gtfs_reload_interval_hours or 24) * 3600
    current_fingerprint: str | None = None

    await asyncio.sleep(interval_seconds)

    while True:
        try:
            logger.info("Checking for updated static GTFS bundle")
            import httpx as _httpx

            response = await asyncio.to_thread(
                lambda: _httpx.get(config.gtfs_static_url, follow_redirects=True, timeout=60)
            )
            response.raise_for_status()
            raw_zip = response.content

            if config.gtfs_static_fix:
                candidate_zip = fix_gtfs_zip(raw_zip)
            else:
                candidate_zip = raw_zip

            feed_info = parse_feed_info(candidate_zip)
            if feed_info is not None:
                new_fingerprint = feed_info.feed_start_date
            else:
                new_fingerprint = hashlib.md5(candidate_zip).hexdigest()

            if new_fingerprint == current_fingerprint:
                logger.info(
                    "Static GTFS unchanged (%s); skipping reload",
                    current_fingerprint,
                )
            else:
                logger.info(
                    "New static GTFS detected (old=%s, new=%s); reloading",
                    current_fingerprint,
                    new_fingerprint,
                )

                if config.gtfs_static_fix:
                    if not config.s3_bucket:
                        raise ValueError(
                            "NIBBLE_S3_BUCKET must be set when NIBBLE_GTFS_STATIC_FIX=true"
                        )
                    from datetime import date

                    from nibble.gtfs.feed_info import FeedInfo
                    from nibble.gtfs.publisher import publish_gtfs_to_s3

                    if feed_info is None:
                        today = date.today().strftime("%Y%m%d")
                        feed_info = FeedInfo(
                            feed_start_date=today,
                            feed_end_date=today,
                            feed_version="unknown",
                        )
                    slug = config.s3_agency_slug
                    publish_gtfs_to_s3(
                        zip_bytes=candidate_zip,
                        feed_info=feed_info,
                        bucket=config.s3_bucket,
                        prefix=f"{slug}/{config.s3_prefix}" if slug else config.s3_prefix,
                        archived_feeds_key=f"{slug}/{config.s3_archived_feeds_key}"
                        if slug
                        else config.s3_archived_feeds_key,
                        region=config.s3_region,
                        archive_url_base="/gtfs",
                    )

                holder.gtfs = load_static_gtfs_from_bytes(candidate_zip)

                current_fingerprint = new_fingerprint
                logger.info("Static GTFS reloaded successfully")

        except Exception:
            logger.exception("Error during GTFS reload check")

        await asyncio.sleep(interval_seconds)


def print_openapi() -> None:
    """Print the OpenAPI schema to stdout as JSON.

    Constructs the app with stub dependencies so no live GTFS or network
    access is required. Pipe to a file to save: ``nibble-openapi > openapi.json``
    """
    from unittest.mock import MagicMock

    from pydantic_settings import PydanticBaseSettingsSource

    class _SchemaSettings(Settings):
        """Settings subclass that ignores env vars and .env so any environment works."""

        @classmethod
        def settings_customise_sources(
            _cls,
            _settings_cls: type[Settings],
            init_settings: PydanticBaseSettingsSource,
            **_kwargs: object,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (init_settings,)

    app = create_app(
        _SchemaSettings(gtfs_rt_url="http://x", gtfs_static_url="http://x"),
        Broadcaster(),
        MagicMock(spec=OverrideStore),
        MagicMock(spec=GtfsHolder),
    )
    print(json.dumps(app.openapi(), indent=2))


def main() -> None:
    """Entry point: load config, download static GTFS, start server and poll loop."""
    config = Settings()  # type: ignore[call-arg]
    configure_logging(config)

    if (
        not config.enable_sse
        and not config.publish_vehicle_positions
        and not config.publish_trip_updates
    ):
        raise ValueError(
            "At least one output mode must be enabled "
            "(NIBBLE_ENABLE_SSE, NIBBLE_PUBLISH_VEHICLE_POSITIONS, or NIBBLE_PUBLISH_TRIP_UPDATES)"
        )
    if config.publish_vehicle_positions and not config.s3_bucket:
        raise ValueError("NIBBLE_S3_BUCKET must be set when NIBBLE_PUBLISH_VEHICLE_POSITIONS=true")
    if config.publish_trip_updates and not config.s3_bucket:
        raise ValueError("NIBBLE_S3_BUCKET must be set when NIBBLE_PUBLISH_TRIP_UPDATES=true")

    holder = GtfsHolder(_load_gtfs(config))
    adapter = get_adapter(
        config.adapter,
        config.gtfs_rt_url,
        config.agency_id,
        config.agency_timezone,
    )

    overrides = OverrideStore(config.overrides_path)
    broadcaster = Broadcaster()

    callbacks = []
    if config.publish_vehicle_positions:
        import functools

        from nibble.publishers.vehicle_positions import publish_vehicle_positions

        callbacks.append(
            functools.partial(
                publish_vehicle_positions,
                bucket=config.s3_bucket,
                key=config.vehicle_positions_s3_key,
                region=config.s3_region,
            )
        )
    if config.publish_trip_updates:
        from nibble.publishers.trip_updates import publish_trip_updates

        async def _publish_trip_updates(snapshot: dict) -> None:
            await publish_trip_updates(
                snapshot,
                holder.gtfs,
                bucket=config.s3_bucket,  # type: ignore[arg-type]
                key=config.trip_updates_s3_key,
                region=config.s3_region,
                agency_timezone=config.agency_timezone,
            )

        callbacks.append(_publish_trip_updates)

    on_snapshot = None
    if callbacks:

        async def _on_snapshot(snapshot: dict) -> None:
            for cb in callbacks:
                await cb(snapshot)

        on_snapshot = _on_snapshot

    if config.enable_sse:
        app = create_app(config, broadcaster, overrides, holder, adapter, on_snapshot)
        uvicorn.run(app, host=config.host, port=config.port)
    else:

        async def run() -> None:
            tasks = [
                asyncio.create_task(
                    poll_loop(config, holder, broadcaster, adapter, overrides, on_snapshot)
                )
            ]
            if config.gtfs_reload_interval_hours is not None:
                tasks.append(asyncio.create_task(gtfs_reload_loop(config, holder)))
            try:
                await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run(run())


if __name__ == "__main__":
    main()
