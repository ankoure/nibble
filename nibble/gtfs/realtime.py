"""Async GTFS-RT protobuf feed fetcher."""

from __future__ import annotations

import logging

import httpx
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


async def fetch_feed(
    url: str,
    client: httpx.AsyncClient,
) -> gtfs_realtime_pb2.FeedMessage | None:
    """Fetch and parse a GTFS-RT VehiclePositions protobuf feed.

    All errors (network failure, non-200 response, protobuf parse error) are
    caught and logged, returning ``None`` so the caller can skip the cycle
    without crashing.

    Args:
        url: The GTFS-RT endpoint URL.
        client: Shared async HTTP client with a 30-second timeout.

    Returns:
        A parsed ``FeedMessage``, or ``None`` on any error.
    """
    try:
        response = await client.get(url, timeout=30)
    except httpx.RequestError as exc:
        logger.warning("GTFS-RT request error: %s", exc)
        return None

    if response.status_code != 200:
        logger.warning("GTFS-RT non-200 response: %d from %s", response.status_code, url)
        return None

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except Exception as exc:
        logger.warning("GTFS-RT protobuf parse error: %s", exc)
        return None

    return feed
