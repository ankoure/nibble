"""Schedule-aware stop-gap interpolation between polling intervals."""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Literal

from nibble.gtfs.static import StaticGTFS, _gtfs_time_to_seconds
from nibble.models import Position, StopTime, VehicleEvent
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
        logger.debug(
            "interpolate: trip mismatch vehicle=%s prev_trip=%r curr_trip=%r",
            curr.vehicle_id,
            prev.last_valid_trip_id,
            trip_id,
        )
        return []

    if prev.last_valid_stop_sequence is None or curr.current_stop_sequence is None:
        logger.debug(
            "interpolate: missing stop sequence vehicle=%s trip=%s prev_seq=%r curr_seq=%r",
            curr.vehicle_id,
            trip_id,
            prev.last_valid_stop_sequence,
            curr.current_stop_sequence,
        )
        return []

    prev_seq = prev.last_valid_stop_sequence
    curr_seq = curr.current_stop_sequence

    if curr_seq <= prev_seq:
        # Backwards - likely a new trip, skip interpolation
        logger.debug(
            "interpolate: backwards sequence vehicle=%s trip=%s prev_seq=%d curr_seq=%d",
            curr.vehicle_id,
            trip_id,
            prev_seq,
            curr_seq,
        )
        return []

    seq_delta = curr_seq - prev_seq

    stop_times = gtfs.stop_times.get(trip_id)
    if not stop_times:
        if seq_delta > max_stops:
            logger.debug(
                "interpolate: skipping linear gap vehicle=%s trip=%s seq_delta=%d > max_stops=%d",
                curr.vehicle_id,
                trip_id,
                seq_delta,
                max_stops,
            )
            return []
        return _linear_interpolate(prev, curr, seq_delta)

    # Find the slice of stop_times between prev_seq (exclusive) and curr_seq (inclusive)
    intermediate = [st for st in stop_times if prev_seq < st.stop_sequence <= curr_seq]
    if not intermediate:
        logger.debug(
            "interpolate: no intermediate stops found vehicle=%s trip=%s prev_seq=%d curr_seq=%d",
            curr.vehicle_id,
            trip_id,
            prev_seq,
            curr_seq,
        )
        return []

    if len(intermediate) > max_stops:
        logger.debug(
            "interpolate: skipping gap vehicle=%s trip=%s "
            "actual_stop_count=%d > max_stops=%d (seq_delta=%d)",
            curr.vehicle_id,
            trip_id,
            len(intermediate),
            max_stops,
            seq_delta,
        )
        return []

    # Check for trip terminus: if any stop in the gap has no departure_time it may be a layover
    # We use a simple heuristic: skip if we can't determine timing for any intermediate stop
    prev_time = _state_timestamp(prev) or curr.timestamp - timedelta(seconds=60 * len(intermediate))
    curr_time = curr.timestamp
    total_seconds = (curr_time - prev_time).total_seconds()
    if total_seconds <= 0:
        logger.warning(
            "interpolate: non-positive time window vehicle=%s trip=%s "
            "prev_time=%s curr_time=%s total_seconds=%.1f - skipping",
            curr.vehicle_id,
            trip_id,
            prev_time,
            curr_time,
            total_seconds,
        )
        return []

    # Assign timestamps proportionally using scheduled durations if available
    scheduled_durations = _scheduled_durations(stop_times, prev_seq, curr_seq)
    events: list[VehicleEvent] = []

    for i, st in enumerate(intermediate):  # intermediate is a list, indexable for bearing lookup
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

        if is_last:
            position = curr.position
        else:
            next_stop_id = intermediate[i + 1].stop_id
            position = _position_for_stop(st.stop_id, next_stop_id, gtfs, curr.position)

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
                position=position,
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
    if total_seconds <= 0:
        logger.warning(
            "_linear_interpolate: non-positive time window vehicle=%s "
            "prev_time=%s curr_time=%s total_seconds=%.1f - skipping",
            curr.vehicle_id,
            prev_time,
            curr_time,
            total_seconds,
        )
        return []

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
                current_stop_sequence=prev.last_valid_stop_sequence + i
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
    if not state.last_seen.tzinfo:
        logger.warning(
            "VehicleState.last_seen for %s is timezone-naive; "
            "falling back to 60s-per-stop estimate for interpolation",
            state.vehicle_id,
        )
        return None
    return state.last_seen


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

    # Collect raw times (None where arrival_time and departure_time are both absent)
    raw: list[float | None] = []
    for st in relevant:
        t = _gtfs_time_to_seconds(st.arrival_time or st.departure_time)
        raw.append(float(t) if t is not None else None)

    # Need at least 2 known values to bracket any gaps
    known_count = sum(1 for v in raw if v is not None)
    if known_count < 2:
        return []

    # Fill missing values by linear interpolation between neighboring known times
    filled: list[float | None] = list(raw)
    for i in range(len(filled)):
        if filled[i] is not None:
            continue
        left = next(((j, filled[j]) for j in range(i - 1, -1, -1) if filled[j] is not None), None)
        right = next(
            ((j, filled[j]) for j in range(i + 1, len(filled)) if filled[j] is not None), None
        )
        if left is None or right is None:
            return []
        lj, lv = left
        rj, rv = right
        filled[i] = lv + (rv - lv) * (i - lj) / (rj - lj)  # type: ignore[operator]

    base = filled[0]
    if base is None:
        return []
    durations = [max(0.0, v - base) for v in filled[1:]]  # type: ignore[operator]
    if any(d == 0.0 for d in durations[:-1]):
        logger.debug(
            "_scheduled_durations: non-monotonic scheduled times clamped to 0 "
            "for trip with prev_seq=%d curr_seq=%d",
            prev_seq,
            curr_seq,
        )
    return durations


def _stop_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the initial bearing in degrees (0-359) from (lat1, lon1) to (lat2, lon2)."""
    dlon = math.radians(lon2 - lon1)
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _position_for_stop(
    stop_id: str,
    next_stop_id: str,
    gtfs: StaticGTFS,
    fallback: Position,
) -> Position:
    """Return a Position at the stop's coordinates, or fallback if unavailable.

    Bearing is computed toward next_stop_id when both stops are present in
    gtfs.stops; otherwise bearing is None.  Speed is always 0.0 (the vehicle
    is en-route to this stop, not cruising past it).
    """
    coords = gtfs.stops.get(stop_id)
    if coords is None:
        return fallback
    lat, lon = coords
    bearing: float | None = None
    next_coords = gtfs.stops.get(next_stop_id)
    if next_coords is not None:
        bearing = _stop_bearing(lat, lon, next_coords[0], next_coords[1])
    return Position(latitude=lat, longitude=lon, bearing=bearing, speed=0.0)
