"""Abstract base class for feed adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from nibble.protos import gtfs_realtime_pb2


class BaseAdapter(ABC):
    """Fetches vehicle data and returns it as a GTFS-RT FeedMessage.

    Adapters decouple the poll loop from the wire format of the upstream
    feed. GTFS-RT feeds use ``GtfsRtAdapter``; proprietary JSON APIs (e.g.
    Passio GO!) use a format-specific adapter that converts to FeedMessage
    before the normalizer sees the data.
    """

    @abstractmethod
    async def fetch(self, client: httpx.AsyncClient) -> gtfs_realtime_pb2.FeedMessage | None:
        """Fetch vehicles and return as a FeedMessage, or None on error.

        Implementations should catch all network and parse errors internally
        and return ``None`` so the poll loop can skip the cycle gracefully.

        Args:
            client: Shared async HTTP client for the current poll cycle.

        Returns:
            A parsed ``FeedMessage`` containing vehicle positions, or ``None``
            if the fetch or parse failed.
        """
