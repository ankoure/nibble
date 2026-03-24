"""Publish a GTFS-RT TripUpdates protobuf to S3 after each poll.

Requires boto3 (install nibble[s3]).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from nibble.gtfs.static import _gtfs_time_to_seconds
from nibble.predictions import compute_delay

if TYPE_CHECKING:
    from nibble.gtfs.static import StaticGTFS
    from nibble.models import VehicleEvent

logger = logging.getLogger(__name__)


def _build_feed(
    snapshot: dict[str, VehicleEvent],
    gtfs: StaticGTFS,
    agency_timezone: str | None = None,
) -> bytes:
    """Serialize *snapshot* to a GTFS-RT TripUpdates FeedMessage.

    Vehicles without a ``trip_id`` or ``current_stop_sequence`` are skipped.
    Each active vehicle contributes one ``TripUpdate`` with ``StopTimeUpdate``
    entries for every remaining stop in the trip (including the current stop),
    each carrying the vehicle's current delay propagated forward.

    Args:
        snapshot: Current resolved vehicle state keyed by vehicle_id.
        gtfs: Static GTFS indexes for stop-time lookups.
        agency_timezone: IANA timezone name used for delay computation.

    Returns:
        Serialized protobuf bytes.
    """
    from nibble.protos import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    for vehicle_id, event in snapshot.items():
        if event.trip_id is None or event.current_stop_sequence is None:
            continue

        stop_times = gtfs.stop_times.get(event.trip_id)
        if not stop_times:
            continue

        delay = compute_delay(event, gtfs, agency_timezone)
        if delay is None:
            continue

        remaining = [st for st in stop_times if st.stop_sequence >= event.current_stop_sequence]
        if not remaining:
            continue

        entity = feed.entity.add()
        entity.id = vehicle_id

        tu = entity.trip_update
        tu.trip.trip_id = event.trip_id
        if event.route_id:
            tu.trip.route_id = event.route_id
        if event.direction_id is not None:
            tu.trip.direction_id = event.direction_id

        tu.vehicle.id = vehicle_id
        if event.label:
            tu.vehicle.label = event.label

        tu.timestamp = int(event.timestamp.timestamp())

        for st in remaining:
            sched_secs = _gtfs_time_to_seconds(st.arrival_time or st.departure_time)
            if sched_secs is None:
                continue
            stu = tu.stop_time_update.add()
            stu.stop_sequence = st.stop_sequence
            stu.stop_id = st.stop_id
            stu.arrival.delay = delay
            stu.departure.delay = delay

    return feed.SerializeToString()


async def publish_trip_updates(
    snapshot: dict[str, VehicleEvent],
    gtfs: StaticGTFS,
    bucket: str,
    key: str,
    region: str = "us-east-1",
    agency_timezone: str | None = None,
) -> None:
    """Serialize *snapshot* to a GTFS-RT TripUpdates protobuf and upload to S3.

    Args:
        snapshot: Current resolved vehicle state keyed by vehicle_id.
        gtfs: Static GTFS indexes for stop-time lookups.
        bucket: S3 bucket name.
        key: S3 object key (e.g. ``"trip_updates.pb"``).
        region: AWS region for the S3 client. Defaults to ``"us-east-1"``.
        agency_timezone: IANA timezone name used for delay computation.

    Raises:
        ImportError: If ``boto3`` is not installed (install ``nibble[s3]``).
    """
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("boto3 is required for S3 publishing. Install nibble[s3].") from exc

    pb_bytes = _build_feed(snapshot, gtfs, agency_timezone)

    def _upload() -> None:
        s3 = boto3.client("s3", region_name=region)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=pb_bytes,
            ContentType="application/x-protobuf",
        )
        logger.info(
            "Published TripUpdates to s3://%s/%s (%d vehicles)",
            bucket,
            key,
            len(snapshot),
        )

    await asyncio.to_thread(_upload)
