"""Passio GO! JSON API adapter — converts to GTFS-RT FeedMessage.

Passio GO! returns an array of vehicle objects from its REST API. This
adapter translates each object into a GTFS-RT VehiclePosition entity so it
can flow through nibble's existing normalizer → reconciler pipeline unchanged.

Expected JSON shape (fields used by this adapter):
    [
      {
        "vehicleId": "101",
        "routeId": "R1",
        "tripId": "T123",       # optional
        "lat": 42.3601,
        "lon": -71.0589,
        "heading": 270,          # optional, degrees
        "speed": 12.5,           # optional, m/s
        "lastUpdated": 1712345678  # optional, Unix epoch seconds
      },
      ...
    ]

Fields that are absent or null are omitted from the protobuf message.
"""

from __future__ import annotations

import logging
import time

import httpx
from google.transit import gtfs_realtime_pb2

from nibble.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class PassioAdapter(BaseAdapter):
    """Fetches Passio GO! JSON vehicle data and converts it to a FeedMessage."""

    def __init__(self, url: str, agency_id: str = "") -> None:
        """
        Args:
            url: Passio GO! REST API URL returning a JSON array of vehicles.
            agency_id: Optional agency identifier (reserved for future filtering).
        """
        self._url = url
        self._agency_id = agency_id

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """Fetch Passio GO! JSON and convert to a GTFS-RT FeedMessage.

        Args:
            client: Shared async HTTP client.

        Returns:
            A synthetic ``FeedMessage`` built from the JSON vehicle array, or
            ``None`` on network error, non-200 response, or malformed JSON.
        """
        try:
            response = await client.get(self._url, timeout=30)
        except httpx.RequestError as exc:
            logger.warning("Passio request error: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning("Passio non-200 response: %d from %s", response.status_code, self._url)
            return None

        try:
            vehicles = response.json()
        except Exception as exc:
            logger.warning("Passio JSON parse error: %s", exc)
            return None

        if not isinstance(vehicles, list):
            logger.warning("Passio response is not a list: %r", type(vehicles))
            return None

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())

        for vehicle in vehicles:
            vehicle_id = str(vehicle.get("vehicleId", "")).strip()
            if not vehicle_id:
                continue

            entity = feed.entity.add()
            entity.id = vehicle_id

            vp = entity.vehicle
            vp.vehicle.id = vehicle_id

            route_id = str(vehicle.get("routeId", "")).strip()
            trip_id = str(vehicle.get("tripId", "")).strip()
            if route_id or trip_id:
                if route_id:
                    vp.trip.route_id = route_id
                if trip_id:
                    vp.trip.trip_id = trip_id

            lat = vehicle.get("lat")
            lon = vehicle.get("lon")
            if lat is not None and lon is not None:
                vp.position.latitude = float(lat)
                vp.position.longitude = float(lon)

            heading = vehicle.get("heading")
            if heading is not None:
                vp.position.bearing = float(heading)

            speed = vehicle.get("speed")
            if speed is not None:
                vp.position.speed = float(speed)

            last_updated = vehicle.get("lastUpdated")
            if last_updated is not None:
                vp.timestamp = int(last_updated)

        return feed
