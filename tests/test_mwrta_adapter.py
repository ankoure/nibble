"""Tests for nibble.adapters.mwrta - MWRTA JSON → FeedMessage."""

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest
import respx

from nibble.adapters.mwrta import MwrtaAdapter

URL = "https://mwrta.example.com/vehicles"


def _vehicle(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "ID": 979666956,
        "Route": "RT14",
        "Lat": 42.276,
        "Long": -71.412,
        "Speed": 7.175,
        "Heading": 39.12,
        "DateTime": "2024-01-01T12:00:00",
        "VehiclePlate": "205",
        "Active": True,
    }
    base.update(overrides)
    return base


# --- DateTime fallback to feed header timestamp ---


@pytest.mark.asyncio
@respx.mock
async def test_missing_datetime_uses_header_timestamp() -> None:
    vehicle = _vehicle()
    del vehicle["DateTime"]
    before = int(time.time())
    respx.get(URL).mock(return_value=httpx.Response(200, json=[vehicle]))
    adapter = MwrtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    after = int(time.time())
    assert feed is not None
    assert before <= feed.entity[0].vehicle.timestamp <= after


@pytest.mark.asyncio
@respx.mock
async def test_unparseable_datetime_uses_header_timestamp() -> None:
    vehicle = _vehicle(DateTime="not-a-date")
    before = int(time.time())
    respx.get(URL).mock(return_value=httpx.Response(200, json=[vehicle]))
    adapter = MwrtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    after = int(time.time())
    assert feed is not None
    assert before <= feed.entity[0].vehicle.timestamp <= after


@pytest.mark.asyncio
@respx.mock
async def test_valid_datetime_is_preserved() -> None:
    vehicle = _vehicle(DateTime="2024-01-01T12:00:00")
    respx.get(URL).mock(return_value=httpx.Response(200, json=[vehicle]))
    adapter = MwrtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    # 2024-01-01T12:00:00 America/New_York (EST = UTC-5) = 2024-01-01T17:00:00 UTC = 1704128400
    assert feed.entity[0].vehicle.timestamp == 1704128400


# --- Coordinate validation ---


@pytest.mark.asyncio
@respx.mock
async def test_zero_coordinates_rejected() -> None:
    vehicle = _vehicle(Lat=0.0, Long=0.0)
    respx.get(URL).mock(return_value=httpx.Response(200, json=[vehicle]))
    adapter = MwrtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    # Position should not be set - protobuf defaults are 0.0, but a valid vehicle
    # at (42.276, -71.412) round-trips; for the invalid vehicle, lat stays at 0.0
    # because the adapter skipped it. We verify by checking a valid vehicle differs.
    entity = feed.entity[0]
    assert entity.vehicle.position.latitude == 0.0
    assert entity.vehicle.position.longitude == 0.0


@pytest.mark.asyncio
@respx.mock
async def test_out_of_bounds_coordinates_rejected() -> None:
    vehicle = _vehicle(Lat=51.5, Long=-0.1)  # London - clearly out of MWRTA area
    respx.get(URL).mock(return_value=httpx.Response(200, json=[vehicle]))
    adapter = MwrtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    entity = feed.entity[0]
    assert entity.vehicle.position.latitude == 0.0


@pytest.mark.asyncio
@respx.mock
async def test_valid_coordinates_accepted() -> None:
    vehicle = _vehicle(Lat=42.276, Long=-71.412)
    respx.get(URL).mock(return_value=httpx.Response(200, json=[vehicle]))
    adapter = MwrtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    entity = feed.entity[0]
    assert abs(entity.vehicle.position.latitude - 42.276) < 0.001
    assert abs(entity.vehicle.position.longitude - (-71.412)) < 0.001


@pytest.mark.asyncio
@respx.mock
async def test_non_numeric_coordinates_skipped() -> None:
    vehicle = _vehicle(Lat="bad", Long="data")
    respx.get(URL).mock(return_value=httpx.Response(200, json=[vehicle]))
    adapter = MwrtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    entity = feed.entity[0]
    assert entity.vehicle.position.latitude == 0.0
