"""Vehicle state machine maintaining per-vehicle confidence across polling gaps."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from nibble.gtfs.static import StaticGTFS, infer_stop_from_position, infer_trip_from_position
from nibble.models import Position, VehicleEvent

logger = logging.getLogger(__name__)


@dataclass
class VehicleState:
    """Persisted record of a vehicle's last known good state between polls.

    Holds the most recent valid trip/route/stop values so they can be carried
    forward when a subsequent poll arrives without a trip_id.
    """

    vehicle_id: str
    last_seen: datetime
    confidence: Literal["confirmed", "inferred", "stale"] = "confirmed"
    last_valid_trip_id: str | None = None
    last_valid_route_id: str | None = None
    last_valid_stop_id: str | None = None
    last_valid_stop_sequence: int | None = None
    last_position: Position | None = None


class StateStore:
    """In-memory store that applies the resolution ladder to each incoming vehicle event.

    The resolution ladder (evaluated in order) determines confidence and
    carries forward trip information when a feed temporarily drops trip_id:

    1. trip_id present + found in static GTFS  → confirmed
    2. trip_id present + NOT in static GTFS    → confirmed (feed takes precedence), log warning
    3. trip_id missing, within stale threshold → inferred (carry forward last valid trip info)
    4. trip_id missing, beyond threshold       → stale (reconciler emits remove)
    5. Never seen + no trip_id                 → stale immediately
    """

    def __init__(self, agency_timezone: str | None = None) -> None:
        self._store: dict[str, VehicleState] = {}
        self._agency_timezone = agency_timezone

    def get(self, vehicle_id: str) -> VehicleState | None:
        """Return the stored state for a vehicle, or None if not yet seen.

        Args:
            vehicle_id: The vehicle identifier to look up.

        Returns:
            The stored ``VehicleState``, or ``None`` if the vehicle has not
            appeared in any poll yet.
        """
        return self._store.get(vehicle_id)

    def all(self) -> dict[str, VehicleState]:
        """Return a snapshot of all stored vehicle states.

        Returns:
            A shallow copy of the internal store mapping vehicle IDs to their
            last-known ``VehicleState``.
        """
        return dict(self._store)

    def update_from_event(
        self,
        event: VehicleEvent,
        gtfs: StaticGTFS,
        stale_threshold_seconds: int,
    ) -> VehicleEvent:
        """Apply the resolution ladder and return the event with updated confidence/provenance.

        Resolution ladder (evaluated in order):

        1. trip_id present + found in static GTFS → ``confirmed``
        2. trip_id present + not in static GTFS → ``confirmed`` (feed takes precedence), log warning
        3. trip_id missing, last seen within stale threshold → carry forward, ``inferred``
        4. trip_id missing, beyond stale threshold → ``stale`` (caller emits remove)

        Args:
            event: The raw ``VehicleEvent`` from the current poll.
            gtfs: Static GTFS indexes used to validate ``trip_id`` lookups.
            stale_threshold_seconds: Maximum seconds since last valid observation
                before a vehicle is considered stale.

        Returns:
            A new ``VehicleEvent`` with ``confidence`` and ``provenance`` set
            according to the resolution ladder. The internal store is updated
            as a side effect.
        """
        prev = self._store.get(event.vehicle_id)
        now = event.timestamp

        if event.trip_id:
            if event.trip_id in gtfs.trips:
                confidence: Literal["confirmed", "inferred", "stale"] = "confirmed"
            else:
                logger.warning(
                    "Vehicle %s has trip_id %r not found in static GTFS",
                    event.vehicle_id,
                    event.trip_id,
                )
                confidence = "confirmed"

            # Resolve route_id from static GTFS if not already set
            route_id = event.route_id
            if not route_id and event.trip_id in gtfs.trips:
                route_id = gtfs.trips[event.trip_id].route_id

            # Infer stop data from position when the feed doesn't provide it
            stop_id = event.stop_id
            stop_sequence = event.current_stop_sequence
            current_status = event.current_status
            if stop_id is None and stop_sequence is None:
                inferred_stop_id, inferred_seq, inferred_status = infer_stop_from_position(
                    event.position.latitude, event.position.longitude, event.trip_id, gtfs
                )
                if inferred_stop_id is not None:
                    stop_id = inferred_stop_id
                    stop_sequence = inferred_seq
                    current_status = inferred_status

            updated = VehicleEvent(
                vehicle_id=event.vehicle_id,
                trip_id=event.trip_id,
                route_id=route_id,
                stop_id=stop_id,
                current_stop_sequence=stop_sequence,
                current_status=current_status,
                direction_id=event.direction_id,
                label=event.label,
                position=event.position,
                timestamp=event.timestamp,
                provenance="observed",
                confidence=confidence,
            )
            self._store[event.vehicle_id] = VehicleState(
                vehicle_id=event.vehicle_id,
                last_seen=now,
                confidence=confidence,
                last_valid_trip_id=event.trip_id,
                last_valid_route_id=route_id,
                last_valid_stop_id=stop_id,
                last_valid_stop_sequence=stop_sequence,
                last_position=event.position,
            )
            return updated

        # No trip_id — try to infer from position + route_id before falling back to stale logic
        if event.route_id:
            route_known = event.route_id in gtfs.route_trips
            logger.debug(
                "Vehicle %s: no trip_id, attempting position inference "
                "(route_id=%r, known=%s, lat=%.5f, lon=%.5f)",
                event.vehicle_id,
                event.route_id,
                route_known,
                event.position.latitude,
                event.position.longitude,
            )
            inferred_trip_id = infer_trip_from_position(
                event.position.latitude,
                event.position.longitude,
                event.route_id,
                gtfs,
                timestamp=event.timestamp,
                agency_timezone=self._agency_timezone,
            )
            logger.debug(
                "Vehicle %s: trip inference result -> %r",
                event.vehicle_id,
                inferred_trip_id,
            )
            if inferred_trip_id is not None:
                route_id = event.route_id
                stop_id, stop_sequence, current_status = infer_stop_from_position(
                    event.position.latitude, event.position.longitude, inferred_trip_id, gtfs
                )
                direction_id = gtfs.trips[inferred_trip_id].direction_id
                updated = VehicleEvent(
                    vehicle_id=event.vehicle_id,
                    trip_id=inferred_trip_id,
                    route_id=route_id,
                    stop_id=stop_id,
                    current_stop_sequence=stop_sequence,
                    current_status=current_status if stop_id else event.current_status,
                    direction_id=direction_id,
                    label=event.label,
                    position=event.position,
                    timestamp=event.timestamp,
                    provenance="inferred",
                    confidence="confirmed",
                )
                self._store[event.vehicle_id] = VehicleState(
                    vehicle_id=event.vehicle_id,
                    last_seen=now,
                    confidence="confirmed",
                    last_valid_trip_id=inferred_trip_id,
                    last_valid_route_id=route_id,
                    last_valid_stop_id=stop_id,
                    last_valid_stop_sequence=stop_sequence,
                    last_position=event.position,
                )
                return updated

        # No trip_id and no route_id match — check stale threshold
        if prev is None:
            # Never seen before and no trip_id — treat as stale immediately
            self._store[event.vehicle_id] = VehicleState(
                vehicle_id=event.vehicle_id,
                last_seen=now,
                confidence="stale",
                last_position=event.position,
            )
            return VehicleEvent(
                vehicle_id=event.vehicle_id,
                position=event.position,
                timestamp=event.timestamp,
                provenance="observed",
                confidence="stale",
            )

        elapsed = (now - prev.last_seen).total_seconds()
        if elapsed <= stale_threshold_seconds:
            # Carry forward last known trip info
            updated = VehicleEvent(
                vehicle_id=event.vehicle_id,
                trip_id=prev.last_valid_trip_id,
                route_id=prev.last_valid_route_id,
                stop_id=prev.last_valid_stop_id,
                current_stop_sequence=prev.last_valid_stop_sequence,
                current_status=event.current_status,
                direction_id=event.direction_id,
                label=event.label,
                position=event.position,
                timestamp=event.timestamp,
                provenance="inferred",
                confidence="inferred",
            )
            self._store[event.vehicle_id] = VehicleState(
                vehicle_id=event.vehicle_id,
                last_seen=prev.last_seen,  # don't update last_seen — use original valid time
                confidence="inferred",
                last_valid_trip_id=prev.last_valid_trip_id,
                last_valid_route_id=prev.last_valid_route_id,
                last_valid_stop_id=prev.last_valid_stop_id,
                last_valid_stop_sequence=prev.last_valid_stop_sequence,
                last_position=event.position,
            )
            return updated

        self._store[event.vehicle_id] = VehicleState(
            vehicle_id=event.vehicle_id,
            last_seen=prev.last_seen,
            confidence="stale",
            last_valid_trip_id=prev.last_valid_trip_id,
            last_valid_route_id=prev.last_valid_route_id,
            last_valid_stop_id=prev.last_valid_stop_id,
            last_valid_stop_sequence=prev.last_valid_stop_sequence,
            last_position=event.position,
        )
        return VehicleEvent(
            vehicle_id=event.vehicle_id,
            trip_id=prev.last_valid_trip_id,
            route_id=prev.last_valid_route_id,
            position=event.position,
            timestamp=event.timestamp,
            provenance="observed",
            confidence="stale",
        )

    def remove(self, vehicle_id: str) -> None:
        """Remove a vehicle from the store (called when it disappears from the feed).

        Args:
            vehicle_id: The vehicle identifier to remove. No-op if not present.
        """
        self._store.pop(vehicle_id, None)
