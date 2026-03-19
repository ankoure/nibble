"""Tests for nibble.adapters.routematch - RouteMatch JSON → FeedMessage."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from nibble.adapters.routematch import RouteMatchAdapter, _MPH_TO_MS

URL = "https://routematch.example.com/vehicles"


def _vehicle(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "vehicleId": "2404",
        "latitude": 42.638,
        "longitude": -73.112,
        "heading": 191,
        "speed": 40,
        "masterRouteId": "Wk Rt 01",
        "tripId": "Rte 01 1130 in",
        "lastUpdate": "2026-03-18T11:35:00.000-04:00",
        "deadhead": False,
    }
    base.update(overrides)
    return base


def _response(*vehicles: dict[str, Any]) -> dict[str, Any]:
    return {"data": list(vehicles)}


# --- Basic parsing ---


@pytest.mark.asyncio
@respx.mock
async def test_basic_vehicle_parsed() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(_vehicle())))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 1
    entity = feed.entity[0]
    assert entity.id == "2404"
    vp = entity.vehicle
    assert vp.vehicle.id == "2404"
    assert vp.vehicle.label == "2404"
    assert vp.trip.route_id == "Wk Rt 01"
    assert vp.trip.trip_id == "Rte 01 1130 in"
    assert abs(vp.position.latitude - 42.638) < 0.001
    assert abs(vp.position.longitude - (-73.112)) < 0.001
    assert vp.position.bearing == 191.0


# --- Timestamp parsing ---


@pytest.mark.asyncio
@respx.mock
async def test_last_update_parsed() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(_vehicle())))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    # 2026-03-18T11:35:00.000-04:00 = 2026-03-18T15:35:00Z
    assert feed.entity[0].vehicle.timestamp == 1773848100


@pytest.mark.asyncio
@respx.mock
async def test_missing_last_update_falls_back_to_header() -> None:
    v = _vehicle()
    del v["lastUpdate"]
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(v)))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.timestamp == feed.header.timestamp


# --- Speed conversion (mph → m/s) ---


@pytest.mark.asyncio
@respx.mock
async def test_speed_converted_from_mph() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(_vehicle(speed=40))))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert abs(feed.entity[0].vehicle.position.speed - 40 * _MPH_TO_MS) < 0.01


@pytest.mark.asyncio
@respx.mock
async def test_implausible_speed_skipped() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(_vehicle(speed=200))))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.position.speed == 0.0


# --- Deadhead filtering ---


@pytest.mark.asyncio
@respx.mock
async def test_deadhead_vehicle_skipped() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(_vehicle(deadhead=True))))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 0


# --- Null heading handled ---


@pytest.mark.asyncio
@respx.mock
async def test_null_heading_ignored() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(_vehicle(heading=None))))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.position.bearing == 0.0


# --- Coordinate validation ---


@pytest.mark.asyncio
@respx.mock
async def test_out_of_bounds_coordinates_rejected() -> None:
    v = _vehicle(latitude=51.5, longitude=-0.1)  # London
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(v)))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.position.latitude == 0.0


# --- Missing / malformed responses ---


@pytest.mark.asyncio
@respx.mock
async def test_non_200_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(503))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_invalid_json_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"not json"))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_missing_data_key_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json={"vehicles": []}))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_vehicle_without_id_skipped() -> None:
    v = _vehicle()
    del v["vehicleId"]
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(v)))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 0


@pytest.mark.asyncio
@respx.mock
async def test_multiple_vehicles_parsed() -> None:
    v1 = _vehicle(vehicleId="2404")
    v2 = _vehicle(vehicleId="2401", masterRouteId="Wk Rt 02")
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(v1, v2)))
    adapter = RouteMatchAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 2
    assert {e.id for e in feed.entity} == {"2404", "2401"}
