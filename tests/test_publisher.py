"""Tests for nibble.gtfs.publisher - uses moto to mock S3."""

from __future__ import annotations

import csv
import io

import boto3
from moto import mock_aws

from nibble.gtfs.feed_info import FeedInfo
from nibble.gtfs.publisher import publish_gtfs_to_s3

BUCKET = "test-gtfs-bucket"
PREFIX = "gtfs"
FEEDS_KEY = "archived_feeds.txt"
REGION = "us-east-1"


def _make_feed_info(start: str = "20260101", end: str = "20260331") -> FeedInfo:
    return FeedInfo(feed_start_date=start, feed_end_date=end, feed_version="Test 2026")


def _read_archived_feeds(s3_client: "boto3.client") -> list[dict[str, str]]:
    resp = s3_client.get_object(Bucket=BUCKET, Key=FEEDS_KEY)
    content = resp["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


@mock_aws
def test_publish_creates_zip_and_archived_feeds() -> None:
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=BUCKET)

    zip_bytes = b"FAKE_ZIP_CONTENT"
    feed_info = _make_feed_info("20260101", "20260331")

    url = publish_gtfs_to_s3(
        zip_bytes=zip_bytes,
        feed_info=feed_info,
        bucket=BUCKET,
        prefix=PREFIX,
        archived_feeds_key=FEEDS_KEY,
        region=REGION,
    )

    # ZIP should be uploaded
    zip_key = f"{PREFIX}/20260101.zip"
    resp = s3.get_object(Bucket=BUCKET, Key=zip_key)
    assert resp["Body"].read() == zip_bytes

    # archived_feeds.txt should exist and have the new row
    rows = _read_archived_feeds(s3)
    assert len(rows) == 1
    assert rows[0]["feed_start_date"] == "20260101"
    assert rows[0]["feed_end_date"] == "20260331"
    assert rows[0]["archive_url"] == url
    assert "20260101.zip" in url


@mock_aws
def test_publish_prepends_to_existing_archived_feeds() -> None:
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=BUCKET)

    # Pre-populate archived_feeds.txt with an older entry
    existing = (
        "feed_start_date,feed_end_date,feed_version,archive_url,archive_note\n"
        "20251101,20251231,Fall 2025,https://example.com/fall.zip,\n"
    )
    s3.put_object(Bucket=BUCKET, Key=FEEDS_KEY, Body=existing.encode("utf-8"))

    publish_gtfs_to_s3(
        zip_bytes=b"ZIP",
        feed_info=_make_feed_info("20260101", "20260331"),
        bucket=BUCKET,
        prefix=PREFIX,
        archived_feeds_key=FEEDS_KEY,
        region=REGION,
    )

    rows = _read_archived_feeds(s3)
    assert len(rows) == 2
    # Newest first
    assert rows[0]["feed_start_date"] == "20260101"
    assert rows[1]["feed_start_date"] == "20251101"
