"""Publish a GTFS-RT VehiclePositions protobuf to S3 after each poll.

Requires boto3 (install nibble[s3]).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nibble.models import VehicleEvent

logger = logging.getLogger(__name__)

_STATUS_MAP = {
    "INCOMING_AT": 0,
    "STOPPED_AT": 1,
    "IN_TRANSIT_TO": 2,
}


def _build_feed(snapshot: dict[str, VehicleEvent]) -> bytes:
    """Serialize *snapshot* to a GTFS-RT VehiclePositions FeedMessage.

    Args:
        snapshot: Current vehicle state keyed by vehicle_id.

    Returns:
        Serialized protobuf bytes.
    """
    from nibble.protos import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    for vehicle_id, event in snapshot.items():
        entity = feed.entity.add()
        entity.id = vehicle_id

        vp = entity.vehicle
        vp.vehicle.id = vehicle_id
        if event.label:
            vp.vehicle.label = event.label

        if event.trip_id:
            vp.trip.trip_id = event.trip_id
        if event.route_id:
            vp.trip.route_id = event.route_id
        if event.direction_id is not None:
            vp.trip.direction_id = event.direction_id

        vp.position.latitude = event.position.latitude
        vp.position.longitude = event.position.longitude
        if event.position.bearing is not None:
            vp.position.bearing = event.position.bearing
        if event.position.speed is not None:
            vp.position.speed = event.position.speed

        if event.stop_id:
            vp.stop_id = event.stop_id
        if event.current_stop_sequence is not None:
            vp.current_stop_sequence = event.current_stop_sequence
        vp.current_status = _STATUS_MAP.get(event.current_status, 2)

        vp.timestamp = int(event.timestamp.timestamp())

    return feed.SerializeToString()


async def publish_vehicle_positions(
    snapshot: dict[str, VehicleEvent],
    bucket: str,
    key: str,
    region: str = "us-east-1",
) -> None:
    """Serialize *snapshot* to a GTFS-RT VehiclePositions protobuf and upload to S3.

    Args:
        snapshot: Current vehicle state keyed by vehicle_id.
        bucket: S3 bucket name.
        key: S3 object key (e.g. ``"vehicle_positions.pb"``).
        region: AWS region for the S3 client. Defaults to ``"us-east-1"``.

    Raises:
        ImportError: If ``boto3`` is not installed (install ``nibble[s3]``).
    """
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("boto3 is required for S3 publishing. Install nibble[s3].") from exc

    pb_bytes = _build_feed(snapshot)

    def _upload() -> None:
        s3 = boto3.client("s3", region_name=region)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=pb_bytes,
            ContentType="application/x-protobuf",
        )
        logger.info(
            "Published VehiclePositions to s3://%s/%s (%d vehicles)",
            bucket,
            key,
            len(snapshot),
        )

    await asyncio.to_thread(_upload)
