"""Tests for nibble.adapters.swiv - Swiv JSON → FeedMessage."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from nibble.adapters.swiv import SwivAdapter

URL = "https://swiv.example.com/vehicles"


def _vehicle(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": 2048,
        "numeroEquipement": "1605",
        "type": "Bus",
        "localisation": {"lat": 42.623, "lng": -71.362, "cap": 223},
        "conduite": {
            "idLigne": 27298,
            "vitesse": 7,
            "destination": "Westford Street/Drum Hill",
        },
    }
    base.update(overrides)
    return base


def _response(*vehicles: dict[str, Any]) -> dict[str, Any]:
    return {"vehicule": list(vehicles)}


# --- Basic parsing ---


@pytest.mark.asyncio
@respx.mock
async def test_basic_vehicle_parsed() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(_vehicle())))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 1
    entity = feed.entity[0]
    assert entity.id == "2048"
    vp = entity.vehicle
    assert vp.vehicle.id == "2048"
    assert vp.vehicle.label == "1605"
    assert vp.trip.route_id == "27298"
    assert abs(vp.position.latitude - 42.623) < 0.001
    assert abs(vp.position.longitude - (-71.362)) < 0.001
    assert vp.position.bearing == 223.0


# --- Speed conversion (km/h → m/s) ---


@pytest.mark.asyncio
@respx.mock
async def test_speed_converted_from_kmh() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(_vehicle())))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    # vitesse=7 km/h → ~1.944 m/s
    assert abs(feed.entity[0].vehicle.position.speed - 7 / 3.6) < 0.01


@pytest.mark.asyncio
@respx.mock
async def test_implausible_speed_skipped() -> None:
    v = _vehicle()
    v["conduite"]["vitesse"] = 652
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(v)))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.position.speed == 0.0



@pytest.mark.asyncio
@respx.mock
async def test_non_numeric_coordinates_skipped() -> None:
    v = _vehicle()
    v["localisation"] = {"lat": "bad", "lng": "data", "cap": 0}
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(v)))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.position.latitude == 0.0


# --- Missing / malformed responses ---


@pytest.mark.asyncio
@respx.mock
async def test_non_200_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(503))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_invalid_json_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"not json"))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_missing_vehicule_key_returns_none() -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json={"data": []}))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@pytest.mark.asyncio
@respx.mock
async def test_vehicle_without_id_skipped() -> None:
    v = _vehicle()
    del v["id"]
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(v)))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 0


@pytest.mark.asyncio
@respx.mock
async def test_multiple_vehicles_parsed() -> None:
    v1 = _vehicle(id=1)
    v2 = _vehicle(id=2, numeroEquipement="2507")
    respx.get(URL).mock(return_value=httpx.Response(200, json=_response(v1, v2)))
    adapter = SwivAdapter(url=URL)
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 2
    assert {e.id for e in feed.entity} == {"1", "2"}
