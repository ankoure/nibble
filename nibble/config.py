"""Pydantic Settings loaded from NIBBLE_* environment variables or a .env file."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration.

    All fields are read from environment variables prefixed with ``NIBBLE_``
    (e.g. ``NIBBLE_GTFS_RT_URL``). A ``.env`` file in the working directory
    is also supported.

    Attributes:
        gtfs_rt_url: URL of the GTFS-RT protobuf endpoint. **Required.**
        gtfs_static_url: URL of the static GTFS ZIP archive. **Required.**
        poll_interval_seconds: How often to fetch the feed, in seconds.
            Defaults to ``15``.
        stale_vehicle_threshold_seconds: Seconds after which a vehicle with
            no ``trip_id`` is considered stale and removed. Defaults to ``90``.
        normalizer: Name of the feed normalizer plugin - ``"default"``,
            ``"ripta"``, or ``"mwrta"``. Defaults to ``"default"``.
        max_interpolation_stops: Maximum stop gap size to interpolate.
            Larger gaps are left as-is. Defaults to ``3``.
        host: Bind address for the HTTP server. Defaults to ``"0.0.0.0"``.
        port: Listen port for the HTTP server. Defaults to ``8080``.
        adapter: Feed adapter to use - ``"gtfs_rt"`` (default), ``"passio"``,
            ``"mwrta"``, ``"trillium"``, or ``"swiv"``.
        agency_id: Agency identifier used by JSON adapters such as Passio GO!.
            Defaults to ``""``.
        agency_timezone: IANA timezone name for the agency (e.g.
            ``"America/New_York"``).  When set, trip inference from position
            filters candidates by their scheduled time window before ranking
            by geometry, improving accuracy on routes with multiple trips
            sharing the same shape.  Defaults to ``None`` (geometry-only).
        gtfs_static_fix: When ``True``, download the raw static feed, apply
            CSV fixers, publish the corrected ZIP to S3, and load from the
            fixed bytes. Requires ``s3_bucket``. Defaults to ``False``.
        s3_bucket: S3 bucket name for publishing fixed GTFS. Required when
            ``gtfs_static_fix`` is ``True``.
        s3_agency_slug: Optional slug prepended to both ``s3_prefix`` and
            ``s3_archived_feeds_key``, allowing multiple agencies to share one
            bucket. E.g. ``"mwrta"`` → keys become ``mwrta/gtfs/…`` and
            ``mwrta/archived_feeds.txt``. Defaults to ``None``.
        s3_prefix: S3 key prefix for the published GTFS ZIP.
            Defaults to ``"gtfs"``.
        s3_archived_feeds_key: S3 key for the archived feeds index file.
            Defaults to ``"archived_feeds.txt"``.
        s3_region: AWS region for S3 operations. Defaults to ``"us-east-1"``.
        gtfs_reload_interval_hours: How often to check for a new static GTFS
            bundle, in hours. When set, a background loop re-downloads the
            feed and reloads the in-memory indexes if ``feed_start_date`` has
            changed. Defaults to ``None`` (no reload).
        enable_sse: When ``True`` (the default), start the FastAPI HTTP
            server and serve SSE vehicle-event streams. Set to ``False`` to
            run in a headless polling-only mode (useful when the only output
            is S3 VehiclePositions). Defaults to ``True``.
        publish_vehicle_positions: When ``True``, serialize the current
            vehicle snapshot to a GTFS-RT ``VehiclePositions`` protobuf and
            upload it to S3 after every successful poll. Requires
            ``s3_bucket``. Defaults to ``False``.
        vehicle_positions_s3_key: S3 object key for the published
            ``VehiclePositions`` protobuf. Defaults to
            ``"vehicle_positions.pb"``.
        publish_trip_updates: When ``True``, serialize stop-time predictions
            for all active vehicles to a GTFS-RT ``TripUpdates`` protobuf and
            upload it to S3 after every successful poll. Requires
            ``s3_bucket``. Defaults to ``False``.
        trip_updates_s3_key: S3 object key for the published ``TripUpdates``
            protobuf. Defaults to ``"trip_updates.pb"``.
        overrides_path: Path to the JSON file used to persist manual trip
            assignment overrides across restarts. Defaults to
            ``"overrides.json"`` in the working directory.
    """

    model_config = SettingsConfigDict(env_prefix="NIBBLE_", env_file=".env")

    gtfs_rt_url: str
    gtfs_static_url: str
    poll_interval_seconds: int = 15
    stale_vehicle_threshold_seconds: int = 90
    normalizer: str = "default"
    max_interpolation_stops: int = 3
    host: str = "0.0.0.0"
    port: int = 8080

    adapter: str = "gtfs_rt"
    agency_id: str = ""
    agency_timezone: str | None = None

    gtfs_static_fix: bool = False
    s3_bucket: str | None = None
    s3_agency_slug: str | None = None
    s3_prefix: str = "gtfs"
    s3_archived_feeds_key: str = "archived_feeds.txt"
    s3_region: str = "us-east-1"
    gtfs_reload_interval_hours: int | None = None

    enable_sse: bool = True
    publish_vehicle_positions: bool = False
    vehicle_positions_s3_key: str = "vehicle_positions.pb"
    publish_trip_updates: bool = False
    trip_updates_s3_key: str = "trip_updates.pb"

    log_level: str = "INFO"
    log_json: bool = False

    overrides_path: Path = Path("overrides.json")
