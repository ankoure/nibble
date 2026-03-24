"""Tests for nibble.gtfs.realtime.fetch_feed — uses respx to mock httpx."""

from __future__ import annotations

import httpx
import pytest
import respx
from nibble.protos import gtfs_realtime_pb2

from nibble.gtfs.realtime import fetch_feed

URL = "http://example.com/rt"


def _proto_bytes() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1704067200
    e = feed.entity.add()
    e.id = "e1"
    e.vehicle.vehicle.id = "v1"
    return feed.SerializeToString()


@pytest.mark.asyncio
@respx.mock
async def test_returns_feed_on_200() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=_proto_bytes()))
    async with httpx.AsyncClient() as client:
        result = await fetch_feed(URL, client)
    assert result is not None
    assert len(result.entity) == 1
    assert result.entity[0].vehicle.vehicle.id == "v1"


@pytest.mark.asyncio
@respx.mock
async def test_returns_none_on_non_200() -> None:
    respx.get(URL).mock(return_value=httpx.Response(404))
    async with httpx.AsyncClient() as client:
        result = await fetch_feed(URL, client)
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_returns_none_on_request_error() -> None:
    respx.get(URL).mock(side_effect=httpx.RequestError("connection refused"))
    async with httpx.AsyncClient() as client:
        result = await fetch_feed(URL, client)
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_returns_none_on_protobuf_parse_error() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"not-a-protobuf"))
    async with httpx.AsyncClient() as client:
        result = await fetch_feed(URL, client)
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_logs_warning_on_non_200(caplog: pytest.LogCaptureFixture) -> None:
    respx.get(URL).mock(return_value=httpx.Response(503))
    import logging

    with caplog.at_level(logging.WARNING, logger="nibble.gtfs.realtime"):
        async with httpx.AsyncClient() as client:
            await fetch_feed(URL, client)
    assert any("503" in r.message for r in caplog.records)
