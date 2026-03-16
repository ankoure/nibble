"""Headway computation: vehicle spacing on a route."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from nibble.gtfs.static import _gtfs_time_to_seconds

if TYPE_CHECKING:
    from nibble.gtfs.static import StaticGTFS
    from nibble.models import VehicleEvent

logger = logging.getLogger(__name__)


def compute_headways(
    route_id: str,
    snapshot: dict[str, VehicleEvent],
    gtfs: StaticGTFS,
) -> dict:
    """Compute vehicle headways (spacing) on a route from the current snapshot.

    Vehicles are grouped by direction, sorted furthest-ahead first using
    ``shape_dist_traveled`` at their current stop (falling back to stop sequence).
    Consecutive pairs are annotated with gap metrics.

    Args:
        route_id: The route to compute headways for.
        snapshot: Current resolved vehicle snapshot keyed by vehicle_id.
        gtfs: Static GTFS indexes for stop-time and schedule lookups.

    Returns:
        A dict with keys:
        - ``route_id``: The requested route
        - ``directions``: List of per-direction groups, each with:
            - ``direction_id``: Integer direction id or ``null``
            - ``vehicles``: List of vehicle dicts sorted furthest-ahead first,
              each with ``vehicle_id``, ``trip_id``, ``stop_id``, ``stop_sequence``,
              ``shape_dist_traveled``, ``scheduled_departure``,
              ``gap_to_previous_meters`` (null for lead vehicle),
              ``scheduled_gap_to_previous_seconds`` (null for lead vehicle).
    """
    # Filter to active vehicles on this route with a known trip
    route_vehicles = [
        event
        for event in snapshot.values()
        if event.route_id == route_id and event.trip_id is not None
    ]

    # Group by direction_id
    by_direction: dict[int | None, list[VehicleEvent]] = {}
    for event in route_vehicles:
        key = event.direction_id
        by_direction.setdefault(key, []).append(event)

    directions = []
    for direction_id, vehicles in sorted(
        by_direction.items(), key=lambda kv: (kv[0] is None, kv[0])
    ):
        enriched = [_enrich(v, gtfs) for v in vehicles]
        # Sort furthest ahead first (highest shape_dist_traveled, then stop_sequence)
        enriched.sort(
            key=lambda x: (
                x["shape_dist_traveled"] is None,  # None sorts last
                -(x["shape_dist_traveled"] or 0),
                -(x["stop_sequence"] or 0),
            )
        )

        vehicle_entries = []
        for i, entry in enumerate(enriched):
            if i == 0:
                entry["gap_to_previous_meters"] = None
                entry["scheduled_gap_to_previous_seconds"] = None
            else:
                prev = enriched[i - 1]
                entry["gap_to_previous_meters"] = _gap_meters(prev, entry)
                entry["scheduled_gap_to_previous_seconds"] = _gap_seconds(prev, entry)
            vehicle_entries.append(entry)

        directions.append({"direction_id": direction_id, "vehicles": vehicle_entries})

    return {"route_id": route_id, "directions": directions}


def _enrich(event: VehicleEvent, gtfs: StaticGTFS) -> dict:
    """Build a dict for a vehicle with its resolved position along the route."""
    shape_dist: float | None = None
    scheduled_departure: str | None = None

    stop_times = gtfs.stop_times.get(event.trip_id or "")
    if stop_times and event.current_stop_sequence is not None:
        st = next(
            (s for s in stop_times if s.stop_sequence == event.current_stop_sequence),
            None,
        )
        if st is not None:
            shape_dist = st.shape_dist_traveled
            scheduled_departure = st.departure_time or st.arrival_time

    return {
        "vehicle_id": event.vehicle_id,
        "trip_id": event.trip_id,
        "stop_id": event.stop_id,
        "stop_sequence": event.current_stop_sequence,
        "shape_dist_traveled": shape_dist,
        "scheduled_departure": scheduled_departure,
    }


def _gap_meters(ahead: dict, behind: dict) -> float | None:
    """Distance in meters between two consecutive vehicles along the shape."""
    a = ahead["shape_dist_traveled"]
    b = behind["shape_dist_traveled"]
    if a is None or b is None:
        return None
    return max(0.0, a - b)


def _gap_seconds(ahead: dict, behind: dict) -> int | None:
    """Scheduled time gap in seconds between two consecutive vehicles.

    Computes the difference in scheduled departure times at each vehicle's
    current stop. Positive means the vehicle ahead is scheduled later (normal),
    negative indicates potential bunching.
    """
    a_secs = _gtfs_time_to_seconds(ahead["scheduled_departure"])
    b_secs = _gtfs_time_to_seconds(behind["scheduled_departure"])
    if a_secs is None or b_secs is None:
        return None
    return a_secs - b_secs
