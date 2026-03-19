"""MWRTA JSON API adapter - converts to GTFS-RT FeedMessage.

MWRTA returns an array of vehicle objects from its REST API. This adapter
translates each object into a GTFS-RT VehiclePosition entity.

Expected JSON shape (fields used by this adapter):
    [
      {
        "ID": 979666956,
        "Route": "RT14",
        "Destination": null,
        "Lat": 42.2763303,
        "Long": -71.4119646,
        "Speed": 7.175,
        "Heading": 39.12,
        "DateTime": "2026-03-08T18:33:59",
        "VehiclePlate": "205",
        "Active": true,
        ...
      },
      ...
    ]

Fields that are absent, null, or inactive are omitted or skipped.
"""

from __future__ import annotations

import logging
import time
import zoneinfo
from datetime import datetime, timezone

import httpx
from google.transit import gtfs_realtime_pb2

from nibble.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

# Bounding box for MWRTA's service area (MetroWest Massachusetts, with generous margins)
_LAT_MIN, _LAT_MAX = 41.5, 42.9
_LON_MIN, _LON_MAX = -72.0, -70.5


class MwrtaAdapter(BaseAdapter):
    """Fetches MWRTA JSON vehicle data and converts it to a FeedMessage."""

    def __init__(self, url: str, agency_id: str = "", agency_timezone: str | None = None) -> None:
        self._url = url
        self._agency_id = agency_id
        self._tz = zoneinfo.ZoneInfo(agency_timezone) if agency_timezone else timezone.utc

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        try:
            response = await client.get(self._url, timeout=30)
        except httpx.RequestError as exc:
            logger.warning("MWRTA request error: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning("MWRTA non-200 response: %d from %s", response.status_code, self._url)
            return None

        try:
            vehicles = response.json()
        except Exception as exc:
            logger.warning("MWRTA JSON parse error: %s", exc)
            return None

        if not isinstance(vehicles, list):
            logger.warning("MWRTA response is not a list: %r", type(vehicles))
            return None

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())

        for vehicle in vehicles:
            if not vehicle.get("Active", True):
                continue

            vehicle_id = str(vehicle.get("ID", "")).strip()
            if not vehicle_id:
                continue

            entity = feed.entity.add()
            entity.id = vehicle_id

            vp = entity.vehicle
            vp.vehicle.id = vehicle_id

            plate = str(vehicle.get("VehiclePlate", "")).strip()
            if plate:
                vp.vehicle.label = plate

            route_id = str(vehicle.get("Route", "")).strip()
            destination = vehicle.get("Destination")
            if route_id:
                vp.trip.route_id = route_id
            if destination:
                vp.trip.trip_headsign = str(destination).strip()

            lat = vehicle.get("Lat")
            lon = vehicle.get("Long")
            if lat is not None and lon is not None:
                try:
                    flat, flon = float(lat), float(lon)
                except (TypeError, ValueError):
                    logger.debug(
                        "MWRTA: non-numeric lat/lon %r/%r for ID %s - skipping position",
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
                            "MWRTA: out-of-bounds position lat=%s lon=%s for ID %s - skipping",
                            flat,
                            flon,
                            vehicle_id,
                        )

            heading = vehicle.get("Heading")
            if heading is not None:
                vp.position.bearing = float(heading)

            speed = vehicle.get("Speed")
            if speed is not None:
                vp.position.speed = float(speed)

            date_time = vehicle.get("DateTime")
            if date_time:
                try:
                    dt = datetime.fromisoformat(date_time)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=self._tz)
                    vp.timestamp = int(dt.timestamp())
                except ValueError:
                    logger.debug("MWRTA unparseable DateTime %r for ID %s", date_time, vehicle_id)

            if not vp.timestamp:
                logger.debug(
                    "MWRTA: missing or unparseable DateTime for ID %s, "
                    "falling back to feed header timestamp %d",
                    vehicle_id,
                    feed.header.timestamp,
                )
                vp.timestamp = feed.header.timestamp

        return feed
