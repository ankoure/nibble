"""Serializes internal models to MBTA V3 JSON:API wire format for SSE."""

from __future__ import annotations

import json

from nibble.models import SSEEvent, VehicleEvent


def to_mbta_v3(event: VehicleEvent) -> dict:
    """Translate a VehicleEvent into an MBTA V3 JSON:API vehicle resource object.

    Args:
        event: The vehicle event to serialize.

    Returns:
        A dict with ``"id"``, ``"type"``, ``"attributes"``, and ``"relationships"``
        keys conforming to the MBTA V3 API vehicle resource shape. Relationship
        data is ``None`` when the corresponding ID is unknown.
    """
    attributes: dict = {
        "current_status": event.current_status,
        "current_stop_sequence": event.current_stop_sequence,
        "direction_id": event.direction_id,
        "label": event.label,
        "latitude": event.position.latitude,
        "longitude": event.position.longitude,
        "bearing": event.position.bearing,
        "speed": event.position.speed,
        "updated_at": event.timestamp.isoformat(),
        "occupancy_status": None,
        # nibble-specific provenance metadata
        "provenance": event.provenance,
        "confidence": event.confidence,
    }

    relationships: dict = {}
    if event.trip_id:
        relationships["trip"] = {"data": {"id": event.trip_id, "type": "trip"}}
    else:
        relationships["trip"] = {"data": None}

    if event.route_id:
        relationships["route"] = {"data": {"id": event.route_id, "type": "route"}}
    else:
        relationships["route"] = {"data": None}

    if event.stop_id:
        relationships["stop"] = {"data": {"id": event.stop_id, "type": "stop"}}
    else:
        relationships["stop"] = {"data": None}

    return {
        "id": event.vehicle_id,
        "type": "vehicle",
        "attributes": attributes,
        "relationships": relationships,
    }


def build_sse_payload(sse_event: SSEEvent) -> str:
    """Serialize an SSEEvent to MBTA V3 SSE wire format.

    Args:
        sse_event: The SSE event to serialize.

    Returns:
        A JSON string with ``"event"`` and ``"data"`` keys, suitable for
        writing directly to an ``text/event-stream`` response.
    """
    return json.dumps(
        {
            "event": sse_event.event_type,
            "data": sse_event.data,
        }
    )
