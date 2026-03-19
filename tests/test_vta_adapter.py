"""Tests for nibble.adapters.vta - VTA MyTransitRide JSON → FeedMessage."""

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest
import respx

from nibble.adapters.vta import VtaAdapter, _MPH_TO_MS

URL = "https://vta.mytransitride.com/api/VehicleStatuses?patternIds=1394,1395"


def _vehicle(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "vehicleId": 22,
        "name": "103",
        "patternId": 1401,
        "headsignText": "3",
        "lat": 41.455,
        "lng": -70.601,
        "velocity": 18,
        "bearing": 279,
        "lastUpdate": "2026-03-18T15:46:58",
        "vehicleStateId": 1,
        "bypassDailyTripId": None,
    }
    base.update(overrides)
    return base


# --- Basic parsing ---


@pytest.mark.asyncio
@respx.mock
async def test_basic_vehicle_parsed() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=[_vehicle()]))
    adapter = VtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 1
    entity = feed.entity[0]
    assert entity.id == "22"
    vp = entity.vehicle
    assert vp.vehicle.id == "22"
    assert vp.vehicle.label == "103"
    assert vp.trip.route_id == "3"
    assert abs(vp.position.latitude - 41.455) < 0.001
    assert abs(vp.position.longitude - (-70.601)) < 0.001
    assert vp.position.bearing == 279.0


# --- Timestamp parsing ---


@pytest.mark.asyncio
@respx.mock
async def test_last_update_localized_with_timezone() -> None:
    respx.get(URL).mock(
        return_value=httpx.Response(200, json=[_vehicle(lastUpdate="2026-03-18T15:46:58")])
    )
    adapter = VtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    # 2026-03-18T15:46:58 America/New_York (EDT = UTC-4) = 2026-03-18T19:46:58Z
    assert feed.entity[0].vehicle.timestamp == 1773863218


@pytest.mark.asyncio
@respx.mock
async def test_missing_last_update_falls_back_to_header() -> None:
    v = _vehicle()
    del v["lastUpdate"]
    before = int(time.time())
    respx.get(URL).mock(return_value=httpx.Response(200, json=[v]))
    adapter = VtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    after = int(time.time())
    assert feed is not None
    assert before <= feed.entity[0].vehicle.timestamp <= after


# --- Speed conversion (mph → m/s) ---


@pytest.mark.asyncio
@respx.mock
async def test_speed_converted_from_mph() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=[_vehicle(velocity=18)]))
    adapter = VtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert abs(feed.entity[0].vehicle.position.speed - 18 * _MPH_TO_MS) < 0.01


@pytest.mark.asyncio
@respx.mock
async def test_implausible_speed_skipped() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=[_vehicle(velocity=200)]))
    adapter = VtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.position.speed == 0.0


# --- Coordinate validation ---


@pytest.mark.asyncio
@respx.mock
async def test_out_of_bounds_coordinates_rejected() -> None:
    respx.get(URL).mock(
        return_value=httpx.Response(200, json=[_vehicle(lat=51.5, lng=-0.1)])  # London
    )
    adapter = VtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.position.latitude == 0.0


# --- Missing / malformed responses ---


@pytest.mark.asyncio
@respx.mock
async def test_non_200_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(503))
    adapter = VtaAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_invalid_json_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"not json"))
    adapter = VtaAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_non_list_response_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json={"vehicles": []}))
    adapter = VtaAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_vehicle_without_id_skipped() -> None:
    v = _vehicle()
    del v["vehicleId"]
    respx.get(URL).mock(return_value=httpx.Response(200, json=[v]))
    adapter = VtaAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 0


@pytest.mark.asyncio
@respx.mock
async def test_multiple_vehicles_parsed() -> None:
    v1 = _vehicle(vehicleId=22)
    v2 = _vehicle(vehicleId=23, name="102", headsignText="1")
    respx.get(URL).mock(return_value=httpx.Response(200, json=[v1, v2]))
    adapter = VtaAdapter(url=URL, agency_timezone="America/New_York")
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 2
    assert {e.id for e in feed.entity} == {"22", "23"}
