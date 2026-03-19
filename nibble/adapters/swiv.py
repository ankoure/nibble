"""Swiv JSON API adapter - converts to GTFS-RT FeedMessage.

Swiv returns a JSON object with a ``vehicule`` array from its REST API. This
adapter translates each object into a GTFS-RT VehiclePosition entity.

Expected JSON shape (fields used by this adapter):
    {
      "vehicule": [
        {
          "id": 2048,
          "numeroEquipement": "1605",
          "type": "Bus",
          "localisation": {
            "lat": 42.623,
            "lng": -71.362,
            "cap": 223
          },
          "conduite": {
            "idLigne": 27298,
            "vitesse": 4,
            "destination": "Westford Street/Drum Hill"
          }
        },
        ...
      ]
    }

``vitesse`` is assumed to be in km/h and is converted to m/s. Fields that
are absent or null are omitted from the protobuf message.
"""

from __future__ import annotations

import logging
import time
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
from google.transit import gtfs_realtime_pb2

from nibble.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

# Bounding box covering Massachusetts (with generous margins)
_LAT_MIN, _LAT_MAX = 41.0, 43.5
_LON_MIN, _LON_MAX = -73.5, -69.9

# Sanity cap on speed before unit conversion (km/h); GPS glitches can produce
# absurd values like 652 km/h for a stationary bus.
_MAX_SPEED_KMH = 150.0


class SwivAdapter(BaseAdapter):
    """Fetches Swiv JSON vehicle data and converts it to a FeedMessage."""

    def __init__(self, url: str, agency_id: str = "") -> None:
        self._url = url
        self._agency_id = agency_id

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        parsed = urlparse(self._url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["_tmp"] = [str(int(time.time() * 1000))]
        url = urlunparse(parsed._replace(query=urlencode(params, doseq=True)))

        try:
            response = await client.get(url, timeout=30)
        except httpx.RequestError as exc:
            logger.warning("Swiv request error: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning("Swiv non-200 response: %d from %s", response.status_code, self._url)
            return None

        try:
            data = response.json()
        except Exception as exc:
            logger.warning("Swiv JSON parse error: %s", exc)
            return None

        vehicles = data.get("vehicule") if isinstance(data, dict) else None
        if not isinstance(vehicles, list):
            logger.warning("Swiv response missing 'vehicule' list: %r", type(data))
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

            label = str(vehicle.get("numeroEquipement", "")).strip()
            if label:
                vp.vehicle.label = label

            conduite = vehicle.get("conduite") or {}
            ligne = conduite.get("idLigne")
            if ligne is not None:
                vp.trip.route_id = str(ligne)

            loc = vehicle.get("localisation") or {}
            lat = loc.get("lat")
            lng = loc.get("lng")
            if lat is not None and lng is not None:
                try:
                    flat, flng = float(lat), float(lng)
                except (TypeError, ValueError):
                    logger.debug(
                        "Swiv: non-numeric lat/lng %r/%r for id %s - skipping position",
                        lat,
                        lng,
                        vehicle_id,
                    )
                else:
                    if _LAT_MIN <= flat <= _LAT_MAX and _LON_MIN <= flng <= _LON_MAX:
                        vp.position.latitude = flat
                        vp.position.longitude = flng
                    else:
                        logger.warning(
                            "Swiv: out-of-bounds position lat=%s lng=%s for id %s - skipping",
                            flat,
                            flng,
                            vehicle_id,
                        )

            cap = loc.get("cap")
            if cap is not None:
                vp.position.bearing = float(cap)

            vitesse = conduite.get("vitesse")
            if vitesse is not None:
                try:
                    kmh = float(vitesse)
                except (TypeError, ValueError):
                    pass
                else:
                    if 0.0 <= kmh <= _MAX_SPEED_KMH:
                        vp.position.speed = kmh / 3.6
                    else:
                        logger.debug(
                            "Swiv: implausible vitesse=%s for id %s - skipping speed",
                            vitesse,
                            vehicle_id,
                        )

            vp.timestamp = feed.header.timestamp

        return feed
