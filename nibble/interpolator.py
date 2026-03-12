"""Schedule-aware stop-gap interpolation between polling intervals."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from typing import Literal

from nibble.gtfs.static import StaticGTFS, _gtfs_time_to_seconds
from nibble.models import StopTime, VehicleEvent
from nibble.state import VehicleState

logger = logging.getLogger(__name__)


def interpolate(
    prev: VehicleState,
    curr: VehicleEvent,
    gtfs: StaticGTFS,
    max_stops: int,
) -> list[VehicleEvent]:
    """Produce synthetic VehicleEvent instances for stops between prev and curr.

    Uses scheduled departure/arrival times from static GTFS to distribute
    timestamps proportionally across the gap. Falls back to linear interpolation
    when stop time data is unavailable.

    Args:
        prev: Last known vehicle state from the state store (provides previous
            stop sequence and timestamp).
        curr: Current observed ``VehicleEvent`` (provides current stop sequence,
            position, and trip context).
        gtfs: Static GTFS indexes used to look up scheduled stop times.
        max_stops: Maximum gap size to interpolate. Gaps larger than this are
            returned as an empty list.

    Returns:
        A list of ``VehicleEvent`` objects for each stop in the gap, ending with
        ``curr`` (tagged ``provenance="observed"``). Intermediate stops are tagged
        ``provenance="interpolated"``. Returns an empty list if interpolation is
        not possible or not warranted.
    """
    trip_id = curr.trip_id
    if trip_id is None or prev.last_valid_trip_id != trip_id:
        return []

    if prev.last_valid_stop_sequence is None or curr.current_stop_sequence is None:
        return []

    prev_seq = prev.last_valid_stop_sequence
    curr_seq = curr.current_stop_sequence

    if curr_seq <= prev_seq:
        # Backwards — likely a new trip, skip interpolation
        return []

    gap = curr_seq - prev_seq
    if gap > max_stops:
        return []

    stop_times = gtfs.stop_times.get(trip_id)
    if not stop_times:
        return _linear_interpolate(prev, curr, gap)

    # Find the slice of stop_times between prev_seq (exclusive) and curr_seq (inclusive)
    intermediate = [st for st in stop_times if prev_seq < st.stop_sequence <= curr_seq]
    if not intermediate:
        return []

    # Check for trip terminus: if any stop in the gap has no departure_time it may be a layover
    # We use a simple heuristic: skip if we can't determine timing for any intermediate stop
    prev_time = _state_timestamp(prev) or curr.timestamp - timedelta(seconds=60 * gap)
    curr_time = curr.timestamp
    total_seconds = (curr_time - prev_time).total_seconds()

    # Assign timestamps proportionally using scheduled durations if available
    scheduled_durations = _scheduled_durations(stop_times, prev_seq, curr_seq)
    events: list[VehicleEvent] = []

    for i, st in enumerate(intermediate):
        if scheduled_durations:
            frac = (
                scheduled_durations[i] / scheduled_durations[-1]
                if scheduled_durations[-1]
                else (i + 1) / len(intermediate)
            )
        else:
            frac = (i + 1) / len(intermediate)

        ts = prev_time + timedelta(seconds=total_seconds * frac)
        is_last = i == len(intermediate) - 1
        provenance: Literal["observed", "interpolated"] = "observed" if is_last else "interpolated"
        confidence: Literal["confirmed", "inferred", "stale"] = (
            curr.confidence if is_last else "inferred"
        )

        events.append(
            VehicleEvent(
                vehicle_id=curr.vehicle_id,
                trip_id=trip_id,
                route_id=curr.route_id,
                stop_id=st.stop_id,
                current_stop_sequence=st.stop_sequence,
                current_status="STOPPED_AT"
                if is_last and curr.current_status == "STOPPED_AT"
                else "IN_TRANSIT_TO",
                direction_id=curr.direction_id,
                label=curr.label,
                position=curr.position,
                timestamp=ts,
                provenance=provenance,
                confidence=confidence,
            )
        )

    return events


def _linear_interpolate(prev: VehicleState, curr: VehicleEvent, gap: int) -> list[VehicleEvent]:
    """Fallback: evenly distribute timestamps across the gap when no schedule data exists.

    Args:
        prev: Last known vehicle state (provides the base timestamp).
        curr: Current observed event (provides trip context and position).
        gap: Number of stops to fill in between prev and curr.

    Returns:
        A list of ``gap`` synthetic ``VehicleEvent`` objects with evenly spaced
        timestamps, ending with ``curr`` tagged ``provenance="observed"``.
    """
    prev_time = _state_timestamp(prev) or curr.timestamp - timedelta(seconds=60 * gap)
    curr_time = curr.timestamp
    total_seconds = (curr_time - prev_time).total_seconds()

    events: list[VehicleEvent] = []
    for i in range(1, gap + 1):
        frac = i / gap
        ts = prev_time + timedelta(seconds=total_seconds * frac)
        is_last = i == gap
        events.append(
            VehicleEvent(
                vehicle_id=curr.vehicle_id,
                trip_id=curr.trip_id,
                route_id=curr.route_id,
                stop_id=curr.stop_id if is_last else None,
                current_stop_sequence=(prev.last_valid_stop_sequence or 0) + i
                if prev.last_valid_stop_sequence is not None
                else None,
                current_status=curr.current_status if is_last else "IN_TRANSIT_TO",
                direction_id=curr.direction_id,
                label=curr.label,
                position=curr.position,
                timestamp=ts,
                provenance="observed" if is_last else "interpolated",
                confidence=curr.confidence if is_last else "inferred",
            )
        )
    return events


def _state_timestamp(state: VehicleState) -> datetime | None:
    """Return state.last_seen if it is timezone-aware, else None."""
    return state.last_seen if state.last_seen.tzinfo else None


def _scheduled_durations(stop_times: list[StopTime], prev_seq: int, curr_seq: int) -> list[float]:
    """Return cumulative scheduled seconds for each stop from prev_seq+1 to curr_seq.

    Args:
        stop_times: All stop times for the trip, sorted by stop_sequence.
        prev_seq: The stop sequence of the last observed stop (exclusive lower bound).
        curr_seq: The stop sequence of the current observed stop (inclusive upper bound).

    Returns:
        A list of cumulative elapsed seconds relative to the departure of ``prev_seq``,
        one entry per stop in ``(prev_seq, curr_seq]``. Returns an empty list if
        timing data is unavailable for any stop in the range.
    """
    relevant = [st for st in stop_times if prev_seq <= st.stop_sequence <= curr_seq]
    if len(relevant) < 2:
        return []

    durations: list[float] = []
    base_secs = _gtfs_time_to_seconds(relevant[0].departure_time or relevant[0].arrival_time)
    if base_secs is None:
        return []

    for st in relevant[1:]:
        t = _gtfs_time_to_seconds(st.arrival_time or st.departure_time)
        if t is None:
            return []
        durations.append(max(0.0, t - base_secs))

    return durations
