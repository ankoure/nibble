"""GTFS-RT adapter - wraps the existing fetch_feed() function."""

from __future__ import annotations

import httpx
from nibble.protos import gtfs_realtime_pb2

from nibble.adapters.base import BaseAdapter
from nibble.gtfs.realtime import fetch_feed


class GtfsRtAdapter(BaseAdapter):
    """Fetches a standard GTFS-RT VehiclePositions protobuf feed."""

    def __init__(self, url: str) -> None:
        """
        Args:
            url: GTFS-RT VehiclePositions endpoint URL.
        """
        self._url = url

    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """Fetch and return the GTFS-RT feed, delegating to :func:`~nibble.gtfs.realtime.fetch_feed`.

        Args:
            client: Shared async HTTP client.

        Returns:
            A parsed ``FeedMessage``, or ``None`` on error.
        """
        return await fetch_feed(self._url, client)
