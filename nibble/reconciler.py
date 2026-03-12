"""Diff engine: compares vehicle snapshots and emits SSE events."""

from __future__ import annotations

import logging

from typing import Any

from nibble.config import Settings
from nibble.emitter import to_mbta_v3
from nibble.gtfs.static import StaticGTFS
from nibble.interpolator import interpolate
from nibble.models import SSEEvent, VehicleEvent
from nibble.state import StateStore

logger = logging.getLogger(__name__)


def reconcile(
    prev: dict[str, VehicleEvent],
    curr: dict[str, VehicleEvent],
    state_store: StateStore,
    gtfs: StaticGTFS,
    config: Settings,
) -> list[SSEEvent]:
    """Diff prev and curr snapshots and return SSE events.

    On the first call (empty ``prev``), emits a single ``"reset"`` event containing
    all current vehicles. Subsequent calls emit ``"update"`` for new or changed
    vehicles and ``"remove"`` for vehicles that have disappeared or gone stale.

    Args:
        prev: Vehicle snapshot from the previous poll cycle (empty dict on first call).
        curr: Vehicle snapshot from the current poll cycle, keyed by vehicle ID.
        state_store: Mutable state machine applied to each current vehicle to resolve
            confidence and carry forward trip information.
        gtfs: Static GTFS indexes used by the state machine for trip validation.
        config: Application settings (stale threshold, interpolation limits).

    Returns:
        A list of ``SSEEvent`` objects ready to broadcast. May be empty if nothing
        changed and no stale vehicles were detected.
    """
    # Apply state machine resolution to all current vehicles
    resolved: dict[str, VehicleEvent] = {}
    for vehicle_id, event in curr.items():
        resolved_event = state_store.update_from_event(
            event, gtfs, config.stale_vehicle_threshold_seconds
        )
        resolved[vehicle_id] = resolved_event

    if not prev:
        data = [to_mbta_v3(e) for e in resolved.values()]
        return [SSEEvent(event_type="reset", data=data)]

    events: list[SSEEvent] = []

    removed_ids = set(prev.keys()) - set(curr.keys())
    for vehicle_id in removed_ids:
        prev_state = state_store.get(vehicle_id)
        # Only emit remove if vehicle was previously known with non-stale confidence
        if prev_state and prev_state.confidence != "stale":
            logger.debug("Vehicle %s removed from feed", vehicle_id)
        state_store.remove(vehicle_id)
        events.append(SSEEvent(event_type="remove", data=[{"id": vehicle_id}]))

    update_data: list[dict[str, Any]] = []
    for vehicle_id, curr_event in resolved.items():
        if curr_event.confidence == "stale":
            # Stale vehicles are silently dropped — they'll be removed next cycle
            state_store.remove(vehicle_id)
            events.append(SSEEvent(event_type="remove", data=[{"id": vehicle_id}]))
            continue

        prev_event = prev.get(vehicle_id)

        if prev_event is not None and _should_interpolate(prev_event, curr_event, config):
            prev_state = state_store.get(vehicle_id)
            if prev_state is not None:
                interp_events = interpolate(
                    prev_state, curr_event, gtfs, config.max_interpolation_stops
                )
                if interp_events:
                    for ie in interp_events:
                        update_data.append(to_mbta_v3(ie))
                    continue

        update_data.append(to_mbta_v3(curr_event))

    if update_data:
        events.append(SSEEvent(event_type="update", data=update_data))

    return events


def _should_interpolate(prev: VehicleEvent, curr: VehicleEvent, config: Settings) -> bool:
    """Return True if the stop gap between prev and curr warrants interpolation.

    All of the following must hold:

    - Both events have non-stale confidence (``"confirmed"`` or ``"inferred"``)
    - Both events share the same non-``None`` ``trip_id``
    - Both events have a ``current_stop_sequence`` value
    - The stop gap is strictly greater than 1 and at most ``max_interpolation_stops``

    Args:
        prev: The vehicle event from the previous poll cycle.
        curr: The vehicle event from the current poll cycle.
        config: Application settings providing ``max_interpolation_stops``.

    Returns:
        ``True`` if interpolation should be attempted, ``False`` otherwise.
    """
    if prev.confidence not in ("confirmed", "inferred"):
        return False
    if curr.confidence not in ("confirmed", "inferred"):
        return False
    if prev.trip_id != curr.trip_id or prev.trip_id is None:
        return False
    if prev.current_stop_sequence is None or curr.current_stop_sequence is None:
        return False
    gap = curr.current_stop_sequence - prev.current_stop_sequence
    return 1 < gap <= config.max_interpolation_stops
