"""Async polling loop: fetch → normalize → parse → reconcile → broadcast."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx
from google.transit import gtfs_realtime_pb2

from nibble.adapters.base import BaseAdapter
from nibble.config import Settings
from nibble.gtfs.static import StaticGTFS
from nibble.models import Position, VehicleEvent
from nibble.normalizer.base import BaseNormalizer
from nibble.reconciler import reconcile
from nibble.state import StateStore

logger = logging.getLogger(__name__)

_NORMALIZER_REGISTRY: dict[str, type[BaseNormalizer]] = {}


def _get_normalizer(name: str) -> BaseNormalizer:
    """Instantiate a normalizer by name.

    Imports are deferred to avoid circular dependencies and keep startup fast.

    Args:
        name: Normalizer identifier — ``"default"`` or ``"ripta"``.

    Returns:
        A ``BaseNormalizer`` instance for the given name.

    Raises:
        ValueError: If ``name`` does not match a known normalizer.
    """
    if name == "default":
        from nibble.normalizer.default import DefaultNormalizer

        return DefaultNormalizer()
    if name == "ripta":
        from nibble.normalizer.ripta import RiptaNormalizer

        return RiptaNormalizer()
    raise ValueError(f"Unknown normalizer: {name!r}")


def _parse_feed(feed: gtfs_realtime_pb2.FeedMessage) -> dict[str, VehicleEvent]:
    """Convert a FeedMessage into a vehicle snapshot dict keyed by vehicle_id.

    Entities without a ``vehicle`` field or a resolvable vehicle ID are skipped.
    Timestamps fall back to the feed header timestamp when absent.

    Args:
        feed: A parsed GTFS-RT ``FeedMessage`` protobuf.

    Returns:
        A dict mapping vehicle IDs to ``VehicleEvent`` objects representing the
        vehicle's state at the time of the feed.
    """
    snapshot: dict[str, VehicleEvent] = {}
    feed_ts = feed.header.timestamp
    feed_time = (
        datetime.fromtimestamp(feed_ts, tz=timezone.utc) if feed_ts else datetime.now(timezone.utc)
    )

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle

        vehicle_id = v.vehicle.id if v.HasField("vehicle") else entity.id
        if not vehicle_id:
            continue

        trip_id = v.trip.trip_id if v.HasField("trip") else None
        route_id = v.trip.route_id if v.HasField("trip") else None
        direction_id = v.trip.direction_id if v.HasField("trip") else None

        lat = v.position.latitude if v.HasField("position") else 0.0
        lon = v.position.longitude if v.HasField("position") else 0.0
        bearing = v.position.bearing if v.HasField("position") and v.position.bearing else None
        speed = v.position.speed if v.HasField("position") and v.position.speed else None

        ts = datetime.fromtimestamp(v.timestamp, tz=timezone.utc) if v.timestamp else feed_time
        label = v.vehicle.label if v.HasField("vehicle") and v.vehicle.label else None
        stop_id = v.stop_id if v.stop_id else None
        seq = v.current_stop_sequence if v.current_stop_sequence else None

        status_map = {
            0: "INCOMING_AT",
            1: "STOPPED_AT",
            2: "IN_TRANSIT_TO",
        }
        current_status = status_map.get(v.current_status, "IN_TRANSIT_TO")

        snapshot[vehicle_id] = VehicleEvent(
            vehicle_id=vehicle_id,
            trip_id=trip_id or None,
            route_id=route_id or None,
            stop_id=stop_id,
            current_stop_sequence=seq,
            current_status=current_status,
            direction_id=direction_id,
            label=label,
            position=Position(latitude=lat, longitude=lon, bearing=bearing, speed=speed),
            timestamp=ts,
        )

    return snapshot


async def poll_loop(
    config: Settings,
    gtfs: StaticGTFS,
    broadcaster: "Broadcaster",  # noqa: F821 — forward ref, defined in server.py
    adapter: BaseAdapter | None = None,
) -> None:
    """Run the feed poll loop forever, broadcasting SSE events on each cycle.

    Handles transient errors (network failures, bad responses, parse errors)
    gracefully — a failed poll is logged and skipped; the loop continues on
    the next interval. Unexpected exceptions are also caught and logged so
    the loop never crashes the server.

    Args:
        config: Application settings providing the poll interval, stale
            threshold, normalizer name, and interpolation limits.
        gtfs: Static GTFS indexes passed through to the state machine and
            interpolator for trip validation and schedule lookups.
        broadcaster: The pub/sub hub to push ``SSEEvent`` objects to after
            each successful poll.
        adapter: Feed adapter to use. If ``None``, a ``GtfsRtAdapter`` is
            created from ``config.gtfs_rt_url`` for backward compatibility.
    """
    if adapter is None:
        from nibble.adapters.gtfs_rt import GtfsRtAdapter

        adapter = GtfsRtAdapter(config.gtfs_rt_url)

    normalizer = _get_normalizer(config.normalizer)
    state_store = StateStore(agency_timezone=config.agency_timezone)
    prev_snapshot: dict[str, VehicleEvent] = {}

    async with httpx.AsyncClient() as client:
        while True:
            try:
                feed = await adapter.fetch(client)
                if feed is not None:
                    feed = normalizer.normalize(feed, gtfs)
                    curr_snapshot = _parse_feed(feed)
                    sse_events = reconcile(prev_snapshot, curr_snapshot, state_store, gtfs, config)
                    if sse_events:
                        await broadcaster.broadcast(sse_events)
                        broadcaster.last_poll_time = datetime.now(timezone.utc)
                    prev_snapshot = curr_snapshot
                    logger.debug("Poll complete: %d vehicles", len(curr_snapshot))
            except Exception:
                logger.exception("Unexpected error in poll loop")

            await asyncio.sleep(config.poll_interval_seconds)
