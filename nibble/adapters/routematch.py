"""RouteMatch JSON API adapter - converts to GTFS-RT FeedMessage.

RouteMatch returns a JSON object with a ``data`` array from its REST API.
This adapter translates each object into a GTFS-RT VehiclePosition entity.

Expected JSON shape (fields used by this adapter):
    {
      "data": [
        {
          "vehicleId": "2404",
          "latitude": 42.638,
          "longitude": -73.112,
          "heading": 191,
          "speed": 40,
          "masterRouteId": "Wk Rt 01",
          "tripId": "Rte 01 1130 in",
          "lastUpdate": "2026-03-18T11:35:00.000-04:00",
          "deadhead": false,
          ...
        },
        ...
      ]
    }

``speed`` is assumed to be in mph and is converted to m/s. ``heading`` may
be null. Deadheading vehicles are skipped. Fields that are absent or null
are omitted from the protobuf message.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

import httpx
from google.transit import gtfs_realtime_pb2

from nibble.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

# Bounding box covering Massachusetts (with generous margins)
_LAT_MIN, _LAT_MAX = 41.0, 43.5
_LON_MIN, _LON_MAX = -73.5, -69.9

# Sanity cap on speed before unit conversion (mph)
_MAX_SPEED_MPH = 100.0
_MPH_TO_MS = 0.44704


class RouteMatchAdapter(BaseAdapter):
    """Fetches RouteMatch JSON vehicle data and converts it to a FeedMessage."""

    def __init__(self, url: str, agency_id: str = "") -> None:
        """
        Args:
            url: RouteMatch REST API endpoint URL.
            agency_id: Unused; kept for interface compatibility.
        """
        self._url = url
        self._agency_id = agency_id

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """GET the RouteMatch vehicle data and convert it to a GTFS-RT FeedMessage.

        Deadheading vehicles are skipped. Speed values (``speed``) are converted
        from mph to m/s; implausible values (> 100 mph) are dropped. Out-of-bounds
        positions are skipped. Falls back to the feed header timestamp when
        ``lastUpdate`` is absent or unparseable.

        Returns:
            A FeedMessage containing one entity per non-deadheading vehicle, or None on error.
        """
        try:
            response = await client.get(self._url, timeout=30)
        except httpx.RequestError as exc:
            logger.warning("RouteMatch request error: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning(
                "RouteMatch non-200 response: %d from %s", response.status_code, self._url
            )
            return None

        try:
            data = response.json()
        except Exception as exc:
            logger.warning("RouteMatch JSON parse error: %s", exc)
            return None

        vehicles = data.get("data") if isinstance(data, dict) else None
        if not isinstance(vehicles, list):
            logger.warning("RouteMatch response missing 'data' list: %r", type(data))
            return None

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())

        for vehicle in vehicles:
            if vehicle.get("deadhead"):
                continue

            vehicle_id = str(vehicle.get("vehicleId", "")).strip()
            if not vehicle_id:
                continue

            entity = feed.entity.add()
            entity.id = vehicle_id

            vp = entity.vehicle
            vp.vehicle.id = vehicle_id
            vp.vehicle.label = vehicle_id

            route_id = str(vehicle.get("masterRouteId", "")).strip()
            if route_id:
                vp.trip.route_id = route_id

            trip_id = str(vehicle.get("tripId", "")).strip()
            if trip_id:
                vp.trip.trip_id = trip_id

            lat = vehicle.get("latitude")
            lon = vehicle.get("longitude")
            if lat is not None and lon is not None:
                try:
                    flat, flon = float(lat), float(lon)
                except (TypeError, ValueError):
                    logger.debug(
                        "RouteMatch: non-numeric lat/lon %r/%r for id %s - skipping position",
                        lat,
                        lon,
                        vehicle_id,
                    )
                else:
                    if _LAT_MIN <= flat <= _LAT_MAX and _LON_MIN <= flon <= _LON_MAX:
                        vp.position.latitude = flat
                        vp.position.longitude = flon
                    else:
                        logger.warning(
                            "RouteMatch: out-of-bounds position lat=%s lon=%s for id %s - skipping",
                            flat,
                            flon,
                            vehicle_id,
                        )

            heading = vehicle.get("heading")
            if heading is not None:
                vp.position.bearing = float(heading)

            speed = vehicle.get("speed")
            if speed is not None:
                try:
                    mph = float(speed)
                except (TypeError, ValueError):
                    pass
                else:
                    if 0.0 <= mph <= _MAX_SPEED_MPH:
                        vp.position.speed = mph * _MPH_TO_MS
                    else:
                        logger.debug(
                            "RouteMatch: implausible speed=%s mph for id %s - skipping speed",
                            speed,
                            vehicle_id,
                        )

            last_update = vehicle.get("lastUpdate")
            if last_update:
                try:
                    vp.timestamp = int(datetime.fromisoformat(last_update).timestamp())
                except (ValueError, TypeError):
                    logger.debug(
                        "RouteMatch: unparseable lastUpdate %r for id %s", last_update, vehicle_id
                    )

            if not vp.timestamp:
                vp.timestamp = feed.header.timestamp

        return feed
