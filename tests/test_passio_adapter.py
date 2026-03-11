"""Tests for nibble.adapters.passio — Passio GO! JSON → FeedMessage."""

from __future__ import annotations


import httpx
import pytest
import respx

from nibble.adapters.passio import PassioAdapter

URL = "https://passio.example.com/vehicles"


def _sample_vehicles() -> list[dict]:
    return [
        {
            "vehicleId": "101",
            "routeId": "R1",
            "tripId": "T123",
            "lat": 42.3601,
            "lon": -71.0589,
            "heading": 270.0,
            "speed": 12.5,
            "lastUpdated": 1712345678,
        },
        {
            "vehicleId": "202",
            "routeId": "R2",
            "lat": 42.3501,
            "lon": -71.0489,
        },
    ]


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_happy_path() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_sample_vehicles()))
    adapter = PassioAdapter(URL)
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
    assert e1.vehicle.timestamp == 1712345678

    # Vehicle with no trip_id should still be included
    e2 = feed.entity[1]
    assert e2.id == "202"
    assert e2.vehicle.trip.route_id == "R2"


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_skips_vehicle_without_id() -> None:
    vehicles = [{"routeId": "R1", "lat": 42.0, "lon": -71.0}]  # no vehicleId
    respx.get(URL).mock(return_value=httpx.Response(200, json=vehicles))
    adapter = PassioAdapter(URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 0


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_returns_none_on_error() -> None:
    respx.get(URL).mock(return_value=httpx.Response(500))
    adapter = PassioAdapter(URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_returns_none_on_bad_json() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"not-json"))
    adapter = PassioAdapter(URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_passio_adapter_returns_none_when_not_list() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json={"vehicles": []}))
    adapter = PassioAdapter(URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None
