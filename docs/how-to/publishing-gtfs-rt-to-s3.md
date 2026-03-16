# Publishing GTFS-RT feeds to S3

After each poll, nibble can re-publish its resolved vehicle state as standard GTFS-RT protobuf feeds to an S3 bucket. This lets downstream systems — other agencies, analytics pipelines, archival tools — consume GTFS-RT without connecting to nibble's SSE stream.

Two feeds are available:

- **VehiclePositions** — every vehicle in the snapshot, with position, trip, and stop status. All vehicles are included regardless of whether a `trip_id` could be resolved.
- **TripUpdates** — vehicles with a resolved `trip_id` and known stop sequence, with a `StopTimeUpdate` for every remaining stop carrying the vehicle's current delay propagated forward. Vehicles without a `trip_id` or `current_stop_sequence` are omitted.

---

## Prerequisites

Install the S3 extra:

```bash
pip install nibble[s3]
```

This adds `boto3` as a dependency. Boto3 picks up AWS credentials from the standard chain: environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), `~/.aws/credentials`, or an EC2/ECS instance profile.

The IAM principal used must have `s3:PutObject` on the target keys:

```json
{
  "Effect": "Allow",
  "Action": "s3:PutObject",
  "Resource": [
    "arn:aws:s3:::my-bucket/gtfs-rt/vehicle_positions.pb",
    "arn:aws:s3:::my-bucket/gtfs-rt/trip_updates.pb"
  ]
}
```

---

## Publishing VehiclePositions

```python
import asyncio
from nibble.vehicle_positions_publisher import publish_vehicle_positions

await publish_vehicle_positions(
    snapshot=snapshot,
    bucket="my-bucket",
    key="gtfs-rt/vehicle_positions.pb",
    region="us-east-1",   # optional, defaults to us-east-1
)
```

Every vehicle in `snapshot` becomes a `VehiclePosition` entity. The internal `current_status` string is mapped to the GTFS-RT enum:

| nibble value | GTFS-RT enum value |
|---|---|
| `"INCOMING_AT"` | `0` |
| `"STOPPED_AT"` | `1` |
| `"IN_TRANSIT_TO"` | `2` (default) |

The feed is uploaded with `Content-Type: application/x-protobuf`.

---

## Publishing TripUpdates

```python
from nibble.trip_updates_publisher import publish_trip_updates

await publish_trip_updates(
    snapshot=snapshot,
    gtfs=gtfs,
    bucket="my-bucket",
    key="gtfs-rt/trip_updates.pb",
    region="us-east-1",             # optional, defaults to us-east-1
    agency_timezone="America/New_York",  # optional, defaults to UTC
)
```

For each vehicle with a resolved `trip_id` and `current_stop_sequence`, nibble:

1. Computes the vehicle's current delay using `compute_delay` (see [Arrival predictions](arrival-predictions.md))
2. Builds a `TripUpdate` with a `StopTimeUpdate` for every remaining stop in the trip
3. Sets `arrival.delay` and `departure.delay` on each stop to the computed delay

Vehicles without a `trip_id`, `current_stop_sequence`, or computable delay are skipped silently.

Pass `agency_timezone` to get correct delay values for agencies not in UTC (see [Arrival predictions — Timezone handling](arrival-predictions.md#timezone-handling)).

---

## Publishing both feeds together

In practice you'll want to publish both feeds on the same cadence as nibble's poll loop:

```python
import asyncio
from nibble.vehicle_positions_publisher import publish_vehicle_positions
from nibble.trip_updates_publisher import publish_trip_updates

BUCKET = "my-bucket"
REGION = "us-east-1"
AGENCY_TZ = "America/New_York"

async def on_poll(snapshot, gtfs):
    await asyncio.gather(
        publish_vehicle_positions(
            snapshot=snapshot,
            bucket=BUCKET,
            key="gtfs-rt/vehicle_positions.pb",
            region=REGION,
        ),
        publish_trip_updates(
            snapshot=snapshot,
            gtfs=gtfs,
            bucket=BUCKET,
            key="gtfs-rt/trip_updates.pb",
            region=REGION,
            agency_timezone=AGENCY_TZ,
        ),
    )
```

Both uploads run concurrently. Each is a single `PutObject` call that overwrites the previous file, so consumers always read the latest snapshot.
