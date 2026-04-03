"""MTA Railroad locations adapter using the backend-unified.mylirr.org API.

Fetches real-time train positions for LIRR (and optionally MNR) from the
MTA Radar backend. The API returns a JSON array of train objects, each with
GPS coordinates, speed, heading, and status. Train numbers map to
``trip_short_name`` in the static GTFS and are rewritten by the
``MtaRailroadNormalizer`` before downstream processing.

Expected response shape (per train):
    {
      "train_num": "1656",
      "realtime": true,
      "location": {
        "latitude": 40.734199,
        "longitude": -73.666574,
        "heading": 72.4,
        "speed": 51.4,          # mph
        "timestamp": 1775247740
      },
      "status": {
        "canceled": false,
        ...
      },
      "details": {
        "stops": [
          {"code": "0NY", "stop_status": "DEPARTED", ...},
          {"code": "0HL", "stop_status": "SCHEDULED", ...},
          ...
        ]
      }
    }
"""

from __future__ import annotations

import logging
import time

import httpx

from nibble.adapters.base import BaseAdapter
from nibble.protos import gtfs_realtime_pb2

logger = logging.getLogger(__name__)

# The API requires this header; without it a 301 is returned indicating the
# correct version. Sourced from browser requests to radar.mta.info.
_ACCEPT_VERSION = "3.0"

# Speed values from the API are in miles per hour; GTFS-RT requires m/s.
_MPH_TO_MS = 0.44704


def _current_stop(stops: list[dict]) -> tuple[str | None, int, int]:
    """Return (stop_code, 1-based sequence, current_status proto int) for the current stop.

    Scans the stops array for the first non-DEPARTED stop. If the train has
    an actual arrival time but no departure time at that stop it is STOPPED_AT;
    otherwise IN_TRANSIT_TO. Returns (None, 0, IN_TRANSIT_TO) when no stop is
    found (empty list or all stops departed).
    """
    for idx, stop in enumerate(stops):
        if stop.get("stop_status") == "DEPARTED":
            continue
        has_arrived = stop.get("act_arrive_time") is not None
        has_departed = stop.get("act_depart_time") is not None
        if has_arrived and not has_departed:
            status = gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
        else:
            status = gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
        return stop.get("code"), idx + 1, status

    return None, 0, gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO


class MyLirrAdapter(BaseAdapter):
    """Fetches MTA Railroad train positions from backend-unified.mylirr.org."""

    def __init__(self, url: str) -> None:
        """
        Args:
            url: Full API URL, e.g.
                ``https://backend-unified.mylirr.org/locations?geometry=TRACK_TURF&railroad=LIRR``
        """
        self._url = url

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """GET the locations endpoint and convert the response to a FeedMessage.

        Trains with no realtime location or a canceled status are skipped.

        Args:
            client: Shared async HTTP client.

        Returns:
            A ``FeedMessage`` containing one VehiclePosition entity per active
            train, or ``None`` on network or parse error.
        """
        try:
            response = await client.get(
                self._url,
                headers={
                    "accept-version": _ACCEPT_VERSION,
                    "origin": "https://radar.mta.info",
                },
                timeout=30,
            )
        except httpx.RequestError as exc:
            logger.warning("MyLIRR request error: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning("MyLIRR non-200 response: %d", response.status_code)
            return None

        try:
            trains = response.json()
        except Exception as exc:
            logger.warning("MyLIRR JSON parse error: %s", exc)
            return None

        if not isinstance(trains, list):
            logger.warning("MyLIRR unexpected response type: %r", type(trains))
            return None

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())

        for train in trains:
            status = train.get("status") or {}
            if status.get("canceled"):
                continue

            location = train.get("location")
            if not location:
                continue

            lat = location.get("latitude")
            lon = location.get("longitude")
            if lat is None or lon is None:
                continue

            train_num = str(train.get("train_num") or "").strip()
            if not train_num:
                continue

            entity = feed.entity.add()
            entity.id = train_num

            vp = entity.vehicle
            vp.vehicle.id = train_num
            vp.vehicle.label = train_num
            # Trip ID is the train number; MtaRailroadNormalizer rewrites it
            # to the full static trip ID via the trip_short_names index.
            vp.trip.trip_id = train_num

            vp.position.latitude = float(lat)
            vp.position.longitude = float(lon)

            heading = location.get("heading")
            if heading is not None:
                vp.position.bearing = float(heading)

            speed = location.get("speed")
            if speed is not None:
                vp.position.speed = float(speed) * _MPH_TO_MS

            ts = location.get("timestamp")
            if ts is not None:
                vp.timestamp = int(ts)

            stop_code, stop_seq, current_status = _current_stop(
                (train.get("details") or {}).get("stops") or []
            )
            if stop_code is not None:
                # Raw stop code (e.g. "0NY"); MtaRailroadNormalizer resolves
                # this to the GTFS stop_id via the stop_codes index.
                vp.stop_id = stop_code
                vp.current_stop_sequence = stop_seq
                vp.current_status = current_status

        return feed
