"""Upload a fixed GTFS ZIP to S3 and maintain archived_feeds.txt for gobble.

The archived_feeds.txt format (CSV, no header quoting):
    feed_start_date,feed_end_date,feed_version,archive_url,archive_note

The most recent entry must appear first. Each call to publish_gtfs_to_s3()
downloads the existing file (if any), prepends the new row, and re-uploads it.

Requires boto3 (install nibble[s3]).
"""

from __future__ import annotations

import csv
import io
import logging

from nibble.gtfs.feed_info import FeedInfo

logger = logging.getLogger(__name__)

_HEADER = ["feed_start_date", "feed_end_date", "feed_version", "archive_url", "archive_note"]


def publish_gtfs_to_s3(
    zip_bytes: bytes,
    feed_info: FeedInfo,
    bucket: str,
    prefix: str,
    archived_feeds_key: str,
    region: str = "us-east-1",
    archive_url_base: str | None = None,
) -> str:
    """Upload the fixed GTFS ZIP and update archived_feeds.txt on S3.

    Returns the archive_url written into archived_feeds.txt.

    Args:
        zip_bytes: Fixed GTFS ZIP content to upload.
        feed_info: Metadata extracted from ``feed_info.txt`` used to name the
            ZIP (``{prefix}/{feed_start_date}.zip``) and populate the index row.
        bucket: S3 bucket name.
        prefix: Key prefix for the ZIP (e.g. ``"gtfs"``).
        archived_feeds_key: Full S3 key for the ``archived_feeds.txt`` index.
        region: AWS region used for the S3 client and to construct the archive URL.
        archive_url_base: When set, the archive_url stored in the index is
            ``{archive_url_base}/{feed_start_date}.zip`` (e.g. ``"/gtfs"``).
            When ``None``, the public S3 HTTPS URL is used instead.

    Returns:
        The archive_url written into archived_feeds.txt.

    Raises:
        ImportError: If ``boto3`` is not installed (install ``nibble[s3]``).
    """
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("boto3 is required for S3 publishing. Install nibble[s3].") from exc

    s3 = boto3.client("s3", region_name=region)

    zip_key = f"{prefix}/{feed_info.feed_start_date}.zip"
    if archive_url_base is not None:
        archive_url = f"{archive_url_base}/{feed_info.feed_start_date}.zip"
    else:
        archive_url = f"https://{bucket}.s3.{region}.amazonaws.com/{zip_key}"

    logger.info("Uploading fixed GTFS ZIP to s3://%s/%s", bucket, zip_key)
    s3.put_object(Bucket=bucket, Key=zip_key, Body=zip_bytes, ContentType="application/zip")

    # Download existing archived_feeds.txt (or start fresh)
    existing_rows: list[list[str]] = []
    try:
        resp = s3.get_object(Bucket=bucket, Key=archived_feeds_key)
        content = resp["Body"].read().decode("utf-8")
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)
        if rows and rows[0] == _HEADER:
            existing_rows = rows[1:]
        else:
            existing_rows = rows
    except s3.exceptions.NoSuchKey:
        logger.info("No existing archived_feeds.txt found; creating new one.")
    except Exception:
        logger.warning("Could not read existing archived_feeds.txt; starting fresh.")

    new_row = [
        feed_info.feed_start_date,
        feed_info.feed_end_date,
        feed_info.feed_version,
        archive_url,
        "",  # archive_note
    ]

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(_HEADER)
    writer.writerow(new_row)
    writer.writerows(existing_rows)

    logger.info("Uploading updated archived_feeds.txt to s3://%s/%s", bucket, archived_feeds_key)
    s3.put_object(
        Bucket=bucket,
        Key=archived_feeds_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    return archive_url


def fetch_fixed_bundle_from_s3(
    feed_start_date: str,
    bucket: str,
    prefix: str,
    region: str = "us-east-1",
) -> bytes | None:
    """Download a previously-fixed GTFS ZIP from S3, or return None if absent.

    Args:
        feed_start_date: Feed start date in ``YYYYMMDD`` format, used as the ZIP key
            (``{prefix}/{feed_start_date}.zip``).
        bucket: S3 bucket name.
        prefix: Key prefix for the ZIP (e.g. ``"gtfs"``).
        region: AWS region used for the S3 client.

    Returns:
        ZIP bytes if the object exists, or ``None`` if it is not found.

    Raises:
        ImportError: If ``boto3`` is not installed (install ``nibble[s3]``).
    """
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("boto3 is required for S3 access. Install nibble[s3].") from exc

    s3 = boto3.client("s3", region_name=region)
    zip_key = f"{prefix}/{feed_start_date}.zip"
    try:
        resp = s3.get_object(Bucket=bucket, Key=zip_key)
        logger.info("Found cached fixed GTFS bundle at s3://%s/%s", bucket, zip_key)
        return resp["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
