# nibble

nibble is a GTFS-RT to MBTA V3 SSE adapter. It sits between raw transit agency realtime feeds and downstream consumers, normalizing inconsistent feed behavior, maintaining vehicle state across polling gaps, and emitting a clean Server-Sent Events stream shaped like the MBTA V3 API.

## The problem it solves

Raw GTFS-RT feeds have a few chronic data quality issues:

- **Silent vehicle drops** — vehicles temporarily lose their `trip_id` when they arrive at a terminus, change trips, or when agency AVL systems glitch. Naive consumers drop and re-add the vehicle, causing UI flickering and lost tracking context.
- **Agency feed quirks** — some agencies (e.g. RIPTA) publish non-standard `trip_id` formats that don't match their own static GTFS. nibble normalizes these before the data reaches consumers.
- **Stop gaps between polls** — polling every 15 seconds means vehicles silently skip stops. nibble interpolates synthetic stop events using the static schedule to fill these gaps.

nibble's output carries `provenance` and `confidence` tags on every vehicle event so consumers can weight or filter data by quality.

## Data flow

```
GTFS-RT protobuf
       |
   fetch_feed          (gtfs/realtime.py)  — async HTTP, graceful error handling
       |
   normalizer          (normalizer/)       — agency-specific feed quirk fixes
       |
   _parse_feed         (poller.py)         — protobuf → VehicleEvent snapshots
       |
   StateStore          (state.py)          — resolution ladder, confidence tagging
       |
   reconcile           (reconciler.py)     — diff prev/curr → SSE events
       |
   interpolate         (interpolator.py)   — fill stop gaps with synthetic events
       |
   Broadcaster         (server.py)         — pub/sub to all SSE clients
       |
  GET /vehicles                            — SSE stream (MBTA V3 format)
```

## Quickstart

### Docker

```sh
docker build -t nibble .
docker run \
  -e NIBBLE_GTFS_RT_URL=https://agency.example/gtfs-rt/VehiclePositions.pb \
  -e NIBBLE_GTFS_STATIC_URL=https://agency.example/gtfs/static.zip \
  -p 8080:8080 \
  nibble
```

### Local development

```sh
uv sync
NIBBLE_GTFS_RT_URL=https://... NIBBLE_GTFS_STATIC_URL=https://... python -m nibble.server
```

## Configuration

All settings are read from `NIBBLE_*` environment variables or a `.env` file in the working directory.

| Variable | Default | Description |
|---|---|---|
| `NIBBLE_GTFS_RT_URL` | **required** | GTFS-RT VehiclePositions protobuf endpoint URL |
| `NIBBLE_GTFS_STATIC_URL` | **required** | Static GTFS ZIP download URL |
| `NIBBLE_POLL_INTERVAL_SECONDS` | `15` | Seconds between feed polls |
| `NIBBLE_STALE_VEHICLE_THRESHOLD_SECONDS` | `90` | Seconds a vehicle can go without a `trip_id` before being dropped |
| `NIBBLE_NORMALIZER` | `"default"` | Feed normalizer to use: `"default"` (pass-through) or `"ripta"` |
| `NIBBLE_MAX_INTERPOLATION_STOPS` | `3` | Maximum stop-sequence gap to interpolate across |
| `NIBBLE_HOST` | `"0.0.0.0"` | Server bind host |
| `NIBBLE_PORT` | `8080` | Server bind port |

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /vehicles` | SSE stream of vehicle events in MBTA V3 format |
| `GET /health` | JSON health check with last poll time and connected client count |

See [API Reference](docs/api.md) for full API reference.

## Running tests

```sh
uv run pytest
```

## Adding a normalizer

1. Create a new file under `nibble/normalizer/`, subclassing `BaseNormalizer`:

   ```python
   from nibble.normalizer.base import BaseNormalizer

   class MyAgencyNormalizer(BaseNormalizer):
       def normalize(self, feed, gtfs):
           # mutate feed as needed
           return feed
   ```

2. Register it in `poller._get_normalizer()`:

   ```python
   if name == "myagency":
       from nibble.normalizer.myagency import MyAgencyNormalizer
       return MyAgencyNormalizer()
   ```

3. Set `NIBBLE_NORMALIZER=myagency` in your environment.

## Project structure

```
nibble/
├── config.py          # Pydantic Settings (NIBBLE_* env vars)
├── models.py          # Core dataclasses: Position, VehicleEvent, SSEEvent, Trip, StopTime
├── state.py           # Vehicle state machine and resolution ladder
├── poller.py          # Async poll loop orchestrator
├── reconciler.py      # Diff engine: prev/curr snapshots → SSE events
├── interpolator.py    # Schedule-aware stop-gap interpolation
├── emitter.py         # MBTA V3 JSON:API serialization
├── server.py          # Starlette SSE server, Broadcaster, health endpoint
├── gtfs/
│   ├── static.py      # Static GTFS ZIP loader and index
│   └── realtime.py    # Async GTFS-RT protobuf fetcher
└── normalizer/
    ├── base.py        # Abstract BaseNormalizer interface
    ├── default.py     # Pass-through normalizer
    └── ripta.py       # RIPTA-specific trip_id normalization
```

For a deep dive into design decisions and the data pipeline, see [ARCHITECTURE.md](ARCHITECTURE.md).
