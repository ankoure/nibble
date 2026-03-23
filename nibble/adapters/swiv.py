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

When a ``topo_url`` is provided, the adapter also fetches the Swiv topology
endpoint on startup and periodically thereafter, parsing the
``idLigne → nomCommercial`` mapping from the ``topo[].ligne[]`` array:

    {
      "topo": [
        {
          "ligne": [
            {
              "idLigne": 27298,
              "nomCommercial": "14",
              ...
            },
            ...
          ]
        },
        ...
      ]
    }

When the map is populated, ``idLigne`` values in the vehicle feed are replaced
with the corresponding ``nomCommercial`` before the FeedMessage is returned,
so downstream normalizers and the state machine see human-readable route IDs.

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

# Sanity cap on speed before unit conversion (km/h); GPS glitches can produce
# absurd values like 652 km/h for a stationary bus.
_MAX_SPEED_KMH = 150.0

# How often to re-fetch the topo endpoint (seconds). Routes change rarely so
# once per day is sufficient; the first fetch happens on the first poll call.
_TOPO_REFRESH_INTERVAL = 86_400


class SwivAdapter(BaseAdapter):
    """Fetches Swiv JSON vehicle data and converts it to a FeedMessage."""

    def __init__(self, url: str, agency_id: str = "", topo_url: str | None = None) -> None:
        """
        Args:
            url: Swiv REST API endpoint URL.
            agency_id: Unused; kept for interface compatibility.
            topo_url: Swiv topology endpoint URL. When ``None``, derived
                automatically by stripping ``/vehicules`` from ``url`` if
                present (the standard Swiv URL convention).
        """
        self._url = url
        self._agency_id = agency_id
        if topo_url is not None:
            self._topo_url: str | None = topo_url
        elif url.rstrip("/").endswith("/vehicules"):
            self._topo_url = url.rstrip("/")[: -len("/vehicules")]
        else:
            self._topo_url = None
        self._ligne_map: dict[str, str] = {}
        self._last_topo_fetch: float = 0.0

    async def _refresh_topo(self, client: httpx.AsyncClient) -> None:
        """Fetch the topo endpoint and rebuild the idLigne → nomCommercial map."""
        assert self._topo_url is not None
        parsed = urlparse(self._topo_url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["_tmp"] = [str(int(time.time() * 1000))]
        url = urlunparse(parsed._replace(query=urlencode(params, doseq=True)))
        try:
            response = await client.get(url, timeout=30)
        except httpx.RequestError as exc:
            logger.warning("Swiv topo request error: %s", exc)
            return

        if response.status_code != 200:
            logger.warning("Swiv topo non-200 response: %d", response.status_code)
            return

        try:
            data = response.json()
        except Exception as exc:
            logger.warning("Swiv topo JSON parse error: %s", exc)
            return

        new_map: dict[str, str] = {}
        for group in data.get("topo", []):
            for ligne in group.get("ligne", []):
                id_ligne = ligne.get("idLigne")
                nom = str(ligne.get("nomCommercial", "")).strip()
                if id_ligne is not None and nom:
                    new_map[str(id_ligne)] = nom

        if new_map:
            self._ligne_map = new_map
            logger.info("Swiv topo: loaded %d idLigne→nomCommercial mappings", len(new_map))
        else:
            logger.warning("Swiv topo: response parsed but no idLigne mappings found")

        self._last_topo_fetch = time.time()

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """GET the Swiv vehicle data and convert it to a GTFS-RT FeedMessage.

        Appends a ``_tmp`` millisecond timestamp to the URL to bust caches.
        Speed values (``vitesse``) are converted from km/h to m/s; implausible
        values (> 150 km/h) are dropped.

        If a ``topo_url`` was provided, the topo mapping is refreshed on the
        first call and every 24 hours thereafter; ``idLigne`` values are
        replaced with the corresponding ``nomCommercial`` in the returned feed.

        Returns:
            A FeedMessage containing one entity per vehicle, or None on error.
        """
        if self._topo_url and (time.time() - self._last_topo_fetch) > _TOPO_REFRESH_INTERVAL:
            await self._refresh_topo(client)

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
                id_str = str(ligne)
                vp.trip.route_id = self._ligne_map.get(id_str, id_str)

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
                    vp.position.latitude = flat
                    vp.position.longitude = flng

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
