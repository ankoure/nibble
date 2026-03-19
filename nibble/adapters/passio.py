"""Passio GO! JSON API adapter - converts to GTFS-RT FeedMessage.

Passio GO! returns vehicle data from a POST endpoint. This adapter posts
{"s0": system_id, "sA": 1} to mapGetData.php?getBuses=2 and translates
the response into a GTFS-RT VehiclePosition FeedMessage.

Expected JSON shape:
    {
      "buses": {
        "<vehicleId>": [
          {
            "busId": "101",
            "routeId": "R1",
            "tripId": "T123",          # may be null
            "latitude": 42.3601,
            "longitude": -71.0589,
            "calculatedCourse": 270,   # may be null
            "speed": 12.5,             # may be null
          }
        ],
        ...
      }
    }

The vehicle ID "-1" is a sentinel used by PassioGO for system metadata and
is skipped.
"""

from __future__ import annotations

import logging
import time

import httpx
from google.transit import gtfs_realtime_pb2

from nibble.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

_ENDPOINT = "https://passiogo.com/mapGetData.php?getBuses=2"


class PassioAdapter(BaseAdapter):
    """Fetches Passio GO! vehicle data via POST and converts it to a FeedMessage."""

    def __init__(self, system_id: str, agency_id: str = "") -> None:
        """
        Args:
            system_id: PassioGO system ID (e.g. "2046" for BAT).
            agency_id: Unused; kept for interface compatibility.
        """
        self._system_id = system_id

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """POST to PassioGO and convert the response to a GTFS-RT FeedMessage.

        Returns:
            A FeedMessage built from the buses dict, or None on error.
        """
        try:
            response = await client.post(
                _ENDPOINT,
                json={"s0": self._system_id, "sA": 1},
                timeout=30,
            )
        except httpx.RequestError as exc:
            logger.warning("Passio request error: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning("Passio non-200 response: %d", response.status_code)
            return None

        try:
            data = response.json()
        except Exception as exc:
            logger.warning("Passio JSON parse error: %s", exc)
            return None

        buses = data.get("buses") if isinstance(data, dict) else None
        if not isinstance(buses, dict):
            logger.warning("Passio response missing 'buses' dict: %r", type(data))
            return None

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())

        for vehicle_id, vehicle_list in buses.items():
            if vehicle_id == "-1":
                continue
            if not vehicle_list:
                continue

            vehicle = vehicle_list[0]
            bus_id = str(vehicle.get("busId") or "").strip()
            if not bus_id:
                continue

            entity = feed.entity.add()
            entity.id = bus_id

            vp = entity.vehicle
            vp.vehicle.id = bus_id

            route_id = str(vehicle.get("routeId") or "").strip()
            trip_id = str(vehicle.get("tripId") or "").strip()
            if route_id:
                vp.trip.route_id = route_id
            if trip_id:
                vp.trip.trip_id = trip_id

            lat = vehicle.get("latitude")
            lon = vehicle.get("longitude")
            if lat is not None and lon is not None:
                vp.position.latitude = float(lat)
                vp.position.longitude = float(lon)

            course = vehicle.get("calculatedCourse")
            if course is not None:
                vp.position.bearing = float(course)

            speed = vehicle.get("speed")
            if speed is not None:
                vp.position.speed = float(speed)

        return feed
