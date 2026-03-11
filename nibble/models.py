"""Core immutable data structures flowing through the nibble pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


@dataclass
class Position:
    """GPS position reported by a vehicle.

    latitude/longitude are WGS84 decimal degrees. bearing is degrees clockwise
    from north (0–359). speed is meters per second. Both bearing and speed may
    be absent from the feed.

    Attributes:
        latitude: WGS84 decimal degrees.
        longitude: WGS84 decimal degrees.
        bearing: Heading in degrees clockwise from north (0–359), or ``None``
            if the feed does not report it.
        speed: Speed in meters per second, or ``None`` if not reported.
    """

    latitude: float
    longitude: float
    bearing: float | None = None
    speed: float | None = None


@dataclass
class VehicleEvent:
    """Immutable snapshot of a vehicle's state at a point in time.

    Every event carries two quality tags:

    **provenance** — how the event was produced:

    - ``"observed"`` — directly reported by the GTFS-RT feed
    - ``"inferred"`` — position observed, but trip info carried forward from a prior poll
    - ``"interpolated"`` — synthetic event generated to fill a stop gap between polls

    **confidence** — certainty about the vehicle's trip assignment:

    - ``"confirmed"`` — vehicle reported a trip_id in this poll
    - ``"inferred"`` — trip_id carried forward; vehicle may have changed trips
    - ``"stale"`` — vehicle exceeded the stale threshold; a remove event follows

    Attributes:
        vehicle_id: Stable identifier for the vehicle (from the GTFS-RT feed).
        position: Current GPS position.
        timestamp: UTC timestamp of this observation.
        provenance: How this event was produced (``"observed"``, ``"inferred"``,
            or ``"interpolated"``).
        confidence: Certainty about the trip assignment (``"confirmed"``,
            ``"inferred"``, or ``"stale"``).
        trip_id: GTFS trip identifier, or ``None`` if unknown.
        route_id: GTFS route identifier, or ``None`` if unknown.
        stop_id: Current stop identifier, or ``None`` if unknown.
        current_stop_sequence: Stop sequence number within the current trip,
            or ``None`` if unknown.
        current_status: Vehicle status relative to the current stop —
            ``"INCOMING_AT"``, ``"STOPPED_AT"``, or ``"IN_TRANSIT_TO"``.
        direction_id: GTFS direction (``0`` or ``1``), or ``None`` if unknown.
        label: Human-readable vehicle label (e.g. bus number), or ``None``.
    """

    vehicle_id: str
    position: Position
    timestamp: datetime
    provenance: Literal["observed", "inferred", "interpolated"] = "observed"
    confidence: Literal["confirmed", "inferred", "stale"] = "confirmed"
    trip_id: str | None = None
    route_id: str | None = None
    stop_id: str | None = None
    current_stop_sequence: int | None = None
    current_status: str = "IN_TRANSIT_TO"
    direction_id: int | None = None
    label: str | None = None


@dataclass
class SSEEvent:
    """An SSE envelope ready for broadcast to clients.

    Attributes:
        event_type: The SSE event name — ``"reset"``, ``"update"``, or ``"remove"``.

            - ``"reset"`` — full snapshot of all known vehicles; sent once to new subscribers
            - ``"update"`` — one or more vehicles changed state; also used for new vehicles
            - ``"remove"`` — one or more vehicles have left the feed; data contains only
              ``{"id": ...}`` objects
        data: List of serialized vehicle dicts (JSON:API format).
    """

    event_type: Literal["reset", "update", "remove"]
    data: list[dict] = field(default_factory=list)


@dataclass
class Trip:
    """Static GTFS trip metadata, loaded from trips.txt.

    Attributes:
        trip_id: Unique GTFS trip identifier.
        route_id: Route this trip belongs to.
        direction_id: Travel direction (``0`` or ``1``), or ``None`` if absent.
        shape_id: Associated shape identifier, or ``None`` if absent.
    """

    trip_id: str
    route_id: str
    direction_id: int | None = None
    shape_id: str | None = None


@dataclass
class StopTime:
    """Scheduled stop entry from stop_times.txt.

    ``arrival_time`` and ``departure_time`` are GTFS HH:MM:SS strings. Hours may
    exceed 23 for service running past midnight (e.g. ``"25:30:00"``).

    Attributes:
        trip_id: Trip this stop time belongs to.
        stop_id: Stop being served.
        stop_sequence: Ordering of the stop within the trip.
        arrival_time: Scheduled arrival as a GTFS HH:MM:SS string, or ``None``.
        departure_time: Scheduled departure as a GTFS HH:MM:SS string, or ``None``.
        shape_dist_traveled: Distance along the shape from the first stop to this
            stop, in the units used by the feed's ``shapes.txt``, or ``None`` if
            not provided.
    """

    trip_id: str
    stop_id: str
    stop_sequence: int
    arrival_time: str | None = None
    departure_time: str | None = None
    shape_dist_traveled: float | None = None
