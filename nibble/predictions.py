"""Stop arrival predictions using current-delay propagation."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from nibble.gtfs.static import _gtfs_time_to_seconds

if TYPE_CHECKING:
    from nibble.gtfs.static import StaticGTFS
    from nibble.models import VehicleEvent

logger = logging.getLogger(__name__)


def compute_delay(
    event: VehicleEvent,
    gtfs: StaticGTFS,
    agency_timezone: str | None = None,
) -> int | None:
    """Return the vehicle's current delay in seconds (positive = late, negative = early).

    Uses the scheduled departure (or arrival) time at the vehicle's current stop
    compared to its observed timestamp. Returns ``None`` if delay cannot be computed
    (missing trip, stop sequence, or schedule data).

    Args:
        event: Current vehicle state with trip_id, current_stop_sequence, and timestamp.
        gtfs: Static GTFS indexes for schedule lookup.
        agency_timezone: IANA timezone name (e.g. ``"America/New_York"``) used to
            convert the vehicle timestamp to local time-of-day. Falls back to UTC.

    Returns:
        Delay in integer seconds, or ``None`` if indeterminate.
    """
    if event.trip_id is None or event.current_stop_sequence is None:
        return None

    stop_times = gtfs.stop_times.get(event.trip_id)
    if not stop_times:
        return None

    current_st = next(
        (st for st in stop_times if st.stop_sequence == event.current_stop_sequence),
        None,
    )
    if current_st is None:
        return None

    scheduled_secs = _gtfs_time_to_seconds(current_st.departure_time or current_st.arrival_time)
    if scheduled_secs is None:
        return None

    actual_secs = _timestamp_to_tod_seconds(event.timestamp, agency_timezone)
    return actual_secs - scheduled_secs


def predict_arrivals(
    event: VehicleEvent,
    gtfs: StaticGTFS,
    agency_timezone: str | None = None,
) -> list[dict]:
    """Return predicted arrival times at remaining stops for a vehicle's current trip.

    Propagates the vehicle's current delay (observed timestamp vs. scheduled departure
    at its current stop) forward to all remaining stops in the trip.

    Args:
        event: Current vehicle state. Must have a non-``None`` ``trip_id`` and
            ``current_stop_sequence``.
        gtfs: Static GTFS indexes used for stop-time lookups.
        agency_timezone: IANA timezone name for local time-of-day conversion.
            Falls back to UTC if ``None`` or unrecognized.

    Returns:
        A list of dicts, one per remaining stop (including the current stop), each with:
        - ``stop_id``: GTFS stop identifier
        - ``stop_sequence``: Stop sequence number in the trip
        - ``scheduled_arrival``: ISO-8601 string of the scheduled arrival time
        - ``predicted_arrival``: ISO-8601 string of the predicted arrival time
        - ``delay_seconds``: Signed delay applied (positive = late)

        Returns an empty list if predictions cannot be computed.
    """
    if event.trip_id is None or event.current_stop_sequence is None:
        return []

    stop_times = gtfs.stop_times.get(event.trip_id)
    if not stop_times:
        return []

    delay = compute_delay(event, gtfs, agency_timezone)
    if delay is None:
        return []

    service_midnight = _service_midnight(event.timestamp, agency_timezone)

    remaining = [st for st in stop_times if st.stop_sequence >= event.current_stop_sequence]

    results = []
    for st in remaining:
        sched_secs = _gtfs_time_to_seconds(st.arrival_time or st.departure_time)
        if sched_secs is None:
            continue
        scheduled_dt = service_midnight + timedelta(seconds=sched_secs)
        predicted_dt = service_midnight + timedelta(seconds=sched_secs + delay)
        results.append(
            {
                "stop_id": st.stop_id,
                "stop_sequence": st.stop_sequence,
                "scheduled_arrival": scheduled_dt.isoformat(),
                "predicted_arrival": predicted_dt.isoformat(),
                "delay_seconds": delay,
            }
        )

    return results


def _timestamp_to_tod_seconds(ts: datetime, agency_timezone: str | None) -> int:
    """Convert a datetime to seconds past midnight in the given timezone (UTC fallback)."""
    local_ts = _to_local(ts, agency_timezone)
    return local_ts.hour * 3600 + local_ts.minute * 60 + local_ts.second


def _service_midnight(ts: datetime, agency_timezone: str | None) -> datetime:
    """Return the UTC-aware datetime of local midnight for the service date of *ts*."""
    local_ts = _to_local(ts, agency_timezone)
    midnight_local = local_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight_local.astimezone(timezone.utc)


def _to_local(ts: datetime, agency_timezone: str | None) -> datetime:
    """Convert *ts* to the given timezone, falling back to UTC."""
    if agency_timezone:
        try:
            from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

            tz = ZoneInfo(agency_timezone)
            return ts.astimezone(tz)
        except (ZoneInfoNotFoundError, KeyError):
            logger.warning("Unknown agency_timezone %r; falling back to UTC", agency_timezone)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)
