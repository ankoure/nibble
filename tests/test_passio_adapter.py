"""Tests for nibble.adapters.passio - Passio GO! JSON → FeedMessage."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from nibble.adapters.passio import PassioAdapter, _ENDPOINT

SYSTEM_ID = "2046"


def _make_response(buses: dict[str, Any]) -> dict[str, Any]:
    return {"buses": buses}


def _bus(
    bus_id: str = "101",
    route_id: str = "R1",
    trip_id: str | None = "T123",
    lat: float = 42.3601,
    lon: float = -71.0589,
    course: float | None = 270.0,
    speed: float | None = 12.5,
) -> list[dict[str, Any]]:
    return [
        {
            "busId": bus_id,
            "routeId": route_id,
            "tripId": trip_id,
            "latitude": lat,
            "longitude": lon,
            "calculatedCourse": course,
            "speed": speed,
        }
    ]


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_happy_path() -> None:
    buses = {
        "101": _bus("101", "R1", "T123", 42.3601, -71.0589, 270.0, 12.5),
        "202": _bus("202", "R2", None, 42.3501, -71.0489, None, None),
    }
    respx.post(_ENDPOINT).mock(return_value=httpx.Response(200, json=_make_response(buses)))
    adapter = PassioAdapter(SYSTEM_ID)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)

    assert feed is not None
    assert len(feed.entity) == 2

    e1 = feed.entity[0]
    assert e1.id == "101"
    assert e1.vehicle.vehicle.id == "101"
    assert e1.vehicle.trip.route_id == "R1"
    assert e1.vehicle.trip.trip_id == "T123"
    assert e1.vehicle.position.latitude == pytest.approx(42.3601)
    assert e1.vehicle.position.longitude == pytest.approx(-71.0589)
    assert e1.vehicle.position.bearing == pytest.approx(270.0)
    assert e1.vehicle.position.speed == pytest.approx(12.5)

    e2 = feed.entity[1]
    assert e2.id == "202"
    assert e2.vehicle.trip.route_id == "R2"
    assert e2.vehicle.trip.trip_id == ""


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_skips_sentinel_vehicle() -> None:
    buses = {
        "-1": _bus("-1"),
        "101": _bus("101"),
    }
    respx.post(_ENDPOINT).mock(return_value=httpx.Response(200, json=_make_response(buses)))
    adapter = PassioAdapter(SYSTEM_ID)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 1
    assert feed.entity[0].id == "101"


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_skips_vehicle_without_bus_id() -> None:
    buses = {"x": [{"routeId": "R1", "latitude": 42.0, "longitude": -71.0}]}
    respx.post(_ENDPOINT).mock(return_value=httpx.Response(200, json=_make_response(buses)))
    adapter = PassioAdapter(SYSTEM_ID)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 0


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_returns_none_on_error() -> None:
    respx.post(_ENDPOINT).mock(return_value=httpx.Response(500))
    adapter = PassioAdapter(SYSTEM_ID)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_returns_none_on_bad_json() -> None:
    respx.post(_ENDPOINT).mock(return_value=httpx.Response(200, content=b"not-json"))
    adapter = PassioAdapter(SYSTEM_ID)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_returns_none_when_no_buses_key() -> None:
    respx.post(_ENDPOINT).mock(return_value=httpx.Response(200, json={"vehicles": []}))
    adapter = PassioAdapter(SYSTEM_ID)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None
