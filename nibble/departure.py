"""Detect trip departures by diffing per-trip stop_time_update head stops across polls."""

from __future__ import annotations

import logging
from datetime import datetime

from nibble.models import VehicleEvent

logger = logging.getLogger(__name__)


def detect_departures(
    prev_heads: dict[str, str],
    curr_heads: dict[str, str],
    vehicles_by_trip: dict[str, VehicleEvent],
    feed_time: datetime,
    stall_threshold_seconds: int,
) -> set[str]:
    """Return trip_ids whose head stop changed and whose matching vehicle is fresh.

    A "departure" is inferred when a trip's ``stop_time_update`` head stop_id
    differs from the previous poll — the former head has been dropped, meaning
    the vehicle has left that stop. If the matching vehicle's timestamp is more
    than ``stall_threshold_seconds`` behind ``feed_time``, the transition is
    treated as a likely data artifact and suppressed (logged, not returned).

    Args:
        prev_heads: Previous poll's mapping of ``trip_id -> head stop_id``.
        curr_heads: Current poll's mapping of ``trip_id -> head stop_id``.
        vehicles_by_trip: Current vehicles keyed by ``trip_id``.
        feed_time: The feed header timestamp for the current poll.
        stall_threshold_seconds: Max allowed lag between vehicle timestamp and
            feed timestamp; exceeding this suppresses the departure.

    Returns:
        The set of trip_ids for which a clean departure was detected.
    """
    departed: set[str] = set()
    for trip_id, curr_head in curr_heads.items():
        prev_head = prev_heads.get(trip_id)
        if prev_head is None or prev_head == curr_head:
            continue
        vehicle = vehicles_by_trip.get(trip_id)
        if vehicle is None:
            continue
        lag = (feed_time - vehicle.timestamp).total_seconds()
        if lag > stall_threshold_seconds:
            logger.info(
                "Suppressed inferred departure for trip %s (stop %s -> %s): "
                "vehicle %s timestamp %.0fs behind feed",
                trip_id,
                prev_head,
                curr_head,
                vehicle.vehicle_id,
                lag,
            )
            continue
        departed.add(trip_id)
    return departed
