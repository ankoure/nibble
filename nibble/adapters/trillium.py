"""Trillium Transit JSON API adapter - converts to GTFS-RT FeedMessage.

Trillium returns a JSON object with a top-level ``data`` array of vehicle
objects. This adapter translates each object into a GTFS-RT VehiclePosition
entity.

Expected JSON shape (fields used by this adapter):
    {
      "status": "success",
      "data": [
        {
          "id": 8819,
          "name": "1204",
          "lat": 42.76554,
          "lon": -71.09184,
          "speed": 0,
          "headingDegrees": 314,
          "lastUpdated": "2026-03-07T23:52:57Z",
          "route_id": "10729",
          "route_short_name": "16",
          "vehicleType": "bus",
          ...
        },
        ...
      ]
    }

Fields that are absent or null are omitted from the protobuf message.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

import httpx
from google.transit import gtfs_realtime_pb2

from nibble.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class TrilliumAdapter(BaseAdapter):
    """Fetches Trillium Transit JSON vehicle data and converts it to a FeedMessage."""

    def __init__(self, url: str, agency_id: str = "") -> None:
        """
        Args:
            url: Trillium Transit JSON API endpoint URL.
            agency_id: Unused; kept for interface compatibility.
        """
        self._url = url
        self._agency_id = agency_id

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """GET the Trillium vehicle data and convert it to a GTFS-RT FeedMessage.

        Extracts vehicles from the top-level ``data`` array. ``lastUpdated``
        timestamps are parsed as ISO 8601 UTC.

        Returns:
            A FeedMessage containing one entity per vehicle, or None on error.
        """
        try:
            response = await client.get(self._url, timeout=30)
        except httpx.RequestError as exc:
            logger.warning("Trillium request error: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning("Trillium non-200 response: %d from %s", response.status_code, self._url)
            return None

        try:
            body = response.json()
        except Exception as exc:
            logger.warning("Trillium JSON parse error: %s", exc)
            return None

        vehicles = body.get("data") if isinstance(body, dict) else body
        if not isinstance(vehicles, list):
            logger.warning("Trillium response has no data list: %r", type(body))
            return None

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())

        for vehicle in vehicles:
            vehicle_id = str(vehicle.get("id", "")).strip()
            if not vehicle_id:
                continue

            entity = feed.entity.add()
            entity.id = vehicle_id

            vp = entity.vehicle
            vp.vehicle.id = vehicle_id

            name = str(vehicle.get("name", "")).strip()
            if name:
                vp.vehicle.label = name

            route_short_name = str(vehicle.get("route_short_name", "")).strip()
            route_id = str(vehicle.get("route_id", "")).strip()
            # Prefer route_short_name over route_id: Trillium's route_id is an
            # internal platform ID that won't match GTFS, while route_short_name
            # (e.g. "16") corresponds to the GTFS route_short_name / route_id.
            effective_route_id = route_short_name or route_id
            if effective_route_id:
                vp.trip.route_id = effective_route_id

            lat = vehicle.get("lat")
            lon = vehicle.get("lon")
            if lat is not None and lon is not None:
                vp.position.latitude = float(lat)
                vp.position.longitude = float(lon)

            heading = vehicle.get("headingDegrees")
            if heading is not None:
                vp.position.bearing = float(heading)

            speed = vehicle.get("speed")
            if speed is not None:
                vp.position.speed = float(speed)

            last_updated = vehicle.get("lastUpdated")
            if last_updated:
                try:
                    dt = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                    vp.timestamp = int(dt.timestamp())
                except ValueError:
                    logger.debug(
                        "Trillium unparseable lastUpdated %r for id %s",
                        last_updated,
                        vehicle_id,
                    )

        return feed
