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

The adapter also fetches the Passio routes endpoint on startup and
periodically thereafter, building a ``myid → display_name`` mapping from
the ``all[]`` array:

    {
      "all": [
        {
          "myid": "50075",
          "shortName": "4",     # GTFS route_short_name; null for named routes
          "name": "Fitchburg 9 & 8",
          ...
        },
        ...
      ]
    }

When the map is populated, ``routeId`` values in the vehicle feed are
replaced with the corresponding ``shortName`` (preferred) or ``name``, so
downstream normalizers and the state machine see GTFS-compatible identifiers.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import httpx

from nibble.adapters.base import BaseAdapter
from nibble.protos import gtfs_realtime_pb2

logger = logging.getLogger(__name__)

_ENDPOINT = "https://passiogo.com/mapGetData.php?getBuses=2"
_ROUTES_ENDPOINT = "https://passiogo.com/mapGetData.php?getRoutes=2"

# How often to re-fetch the routes endpoint (seconds). Routes change rarely.
_ROUTES_REFRESH_INTERVAL = 86_400


class PassioAdapter(BaseAdapter):
    """Fetches Passio GO! vehicle data via POST and converts it to a FeedMessage."""

    def __init__(
        self,
        system_id: str,
        agency_id: str = "",
        static_routes_file: str | None = None,
    ) -> None:
        """
        Args:
            system_id: PassioGO system ID (e.g. "2046" for BAT).
            agency_id: Unused; kept for interface compatibility.
            static_routes_file: Path to a JSON file with the same ``{"all": [...]}``
                structure as the Passio routes endpoint.  Loaded on startup as an
                initial fallback; the live endpoint will overwrite it on the first
                successful fetch.
        """
        self._system_id = system_id
        self._route_map: dict[str, str] = {}  # myid → shortName (or name)
        self._last_routes_fetch: float = 0.0

        if static_routes_file:
            self._load_static_routes(Path(static_routes_file))

    def _load_static_routes(self, path: Path) -> None:
        """Pre-populate ``_route_map`` from a static JSON routes file."""
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Passio static routes: failed to load %s: %s", path, exc)
            return

        routes = data.get("all") if isinstance(data, dict) else None
        if not isinstance(routes, list):
            logger.warning("Passio static routes: missing 'all' list in %s", path)
            return

        new_map: dict[str, str] = {}
        for route in routes:
            myid = str(route.get("myid") or "").strip()
            short_name = str(route.get("shortName") or "").strip()
            name = str(route.get("name") or "").strip()
            if myid and (short_name or name):
                new_map[myid] = short_name if short_name else name

        self._route_map = new_map
        logger.info(
            "Passio static routes: loaded %d myid→route mappings from %s", len(new_map), path
        )

    async def _refresh_routes(self, client: httpx.AsyncClient) -> None:
        """Fetch the routes endpoint and rebuild the myid → display_name map."""
        url = _ROUTES_ENDPOINT
        try:
            response = await client.post(
                url,
                json={"systemSelected0": self._system_id, "amount": 1},
                timeout=30,
            )
        except httpx.RequestError as exc:
            logger.warning("Passio routes request error: %s", exc)
            return

        # Always advance the timestamp so transient failures don't cause a
        # retry on every subsequent poll; the 24-hour interval applies even
        # when the fetch fails.
        self._last_routes_fetch = time.time()

        if response.status_code != 200:
            logger.warning("Passio routes non-200 response: %d", response.status_code)
            return

        try:
            data = response.json()
        except Exception:
            logger.debug(
                "Passio routes: endpoint returned unparseable response "
                "(requires browser session credentials); falling back to 'route' field"
            )
            return

        routes = data.get("all") if isinstance(data, dict) else None
        if not isinstance(routes, list):
            logger.debug("Passio routes response missing 'all' list: %r", type(data))
            return

        new_map: dict[str, str] = {}
        for route in routes:
            myid = str(route.get("myid") or "").strip()
            short_name = str(route.get("shortName") or "").strip()
            name = str(route.get("name") or "").strip()
            if myid and (short_name or name):
                new_map[myid] = short_name if short_name else name

        if new_map:
            self._route_map = new_map
            logger.info("Passio routes: loaded %d myid→route mappings", len(new_map))
        else:
            logger.debug("Passio routes: response parsed but no route mappings found")

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """POST to PassioGO and convert the response to a GTFS-RT FeedMessage.

        Fetches (or refreshes) the routes map before the first poll and every
        24 hours thereafter, then translates ``routeId`` values to
        GTFS-compatible route names.

        Returns:
            A FeedMessage built from the buses dict, or None on error.
        """
        if (time.time() - self._last_routes_fetch) > _ROUTES_REFRESH_INTERVAL:
            await self._refresh_routes(client)

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

            passio_route_id = str(vehicle.get("routeId") or "").strip()
            route_name = str(vehicle.get("route") or "").strip()
            if passio_route_id and self._route_map:
                # Routes endpoint mapped myid → shortName/name
                route_id = self._route_map.get(passio_route_id, route_name or passio_route_id)
            else:
                # Routes endpoint unavailable; use the human-readable name Passio
                # includes in every bus object ("Gardner Route 1 South", "Route 4")
                route_id = route_name or passio_route_id
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
