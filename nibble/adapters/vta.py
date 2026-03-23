"""VTA (Vineyard Transit Authority) JSON API adapter - converts to GTFS-RT FeedMessage.

VTA's MyTransitRide API returns a JSON array of vehicle objects. The request
URL must include the ``patternIds`` query parameter listing the route patterns
to fetch; this is supplied as the configured feed URL.

Expected JSON shape (fields used by this adapter):
    [
      {
        "vehicleId": 22,
        "name": "103",
        "patternId": 1401,
        "headsignText": "3",
        "lat": 41.455,
        "lng": -70.601,
        "velocity": 0,
        "bearing": 279,
        "lastUpdate": "2026-03-18T15:46:58",
        "vehicleStateId": 1
      },
      ...
    ]

``velocity`` is assumed to be in mph and is converted to m/s. ``lastUpdate``
is a naive local datetime; ``agency_timezone`` is required to interpret it.
Fields that are absent or null are omitted from the protobuf message.
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

# Sanity cap on speed (mph)
_MAX_SPEED_MPH = 100.0
_MPH_TO_MS = 0.44704


class VtaAdapter(BaseAdapter):
    """Fetches VTA MyTransitRide JSON vehicle data and converts it to a FeedMessage."""

    def __init__(self, url: str, agency_id: str = "", agency_timezone: str | None = None) -> None:
        """
        Args:
            url: VTA MyTransitRide API endpoint URL (must include ``patternIds`` query param).
            agency_id: Unused; kept for interface compatibility.
            agency_timezone: IANA timezone name (e.g. ``"America/New_York"``) used to
                interpret naive ``lastUpdate`` values.  Defaults to UTC.
        """
        self._url = url
        self._agency_id = agency_id
        self._tz = zoneinfo.ZoneInfo(agency_timezone) if agency_timezone else timezone.utc

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """GET the VTA vehicle data and convert it to a GTFS-RT FeedMessage.

        Speed values (``velocity``) are converted from mph to m/s. ``lastUpdate``
        is a naive local datetime interpreted with the configured agency timezone.

        Returns:
            A FeedMessage containing one entity per vehicle, or None on error.
        """
        try:
            response = await client.get(self._url, timeout=30)
        except httpx.RequestError as exc:
            logger.warning("VTA request error: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning("VTA non-200 response: %d from %s", response.status_code, self._url)
            return None

        try:
            vehicles = response.json()
        except Exception as exc:
            logger.warning("VTA JSON parse error: %s", exc)
            return None

        if not isinstance(vehicles, list):
            logger.warning("VTA response is not a list: %r", type(vehicles))
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

            label = str(vehicle.get("name", "")).strip()
            if label:
                vp.vehicle.label = label

            headsign = str(vehicle.get("headsignText", "")).strip()
            if headsign:
                vp.trip.route_id = headsign

            lat = vehicle.get("lat")
            lng = vehicle.get("lng")
            if lat is not None and lng is not None:
                try:
                    flat, flng = float(lat), float(lng)
                except (TypeError, ValueError):
                    logger.debug(
                        "VTA: non-numeric lat/lng %r/%r for id %s - skipping position",
                        lat,
                        lng,
                        vehicle_id,
                    )
                else:
                    vp.position.latitude = flat
                    vp.position.longitude = flng

            bearing = vehicle.get("bearing")
            if bearing is not None:
                vp.position.bearing = float(bearing)

            velocity = vehicle.get("velocity")
            if velocity is not None:
                try:
                    mph = float(velocity)
                except (TypeError, ValueError):
                    pass
                else:
                    if 0.0 <= mph <= _MAX_SPEED_MPH:
                        vp.position.speed = mph * _MPH_TO_MS
                    else:
                        logger.debug(
                            "VTA: implausible velocity=%s mph for id %s - skipping speed",
                            velocity,
                            vehicle_id,
                        )

            last_update = vehicle.get("lastUpdate")
            if last_update:
                try:
                    dt = datetime.fromisoformat(last_update)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=self._tz)
                    vp.timestamp = int(dt.timestamp())
                except (ValueError, TypeError):
                    logger.debug(
                        "VTA: unparseable lastUpdate %r for id %s", last_update, vehicle_id
                    )

            if not vp.timestamp:
                vp.timestamp = feed.header.timestamp

        return feed
