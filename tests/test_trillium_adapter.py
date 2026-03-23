"""Tests for nibble.adapters.trillium - Trillium JSON → FeedMessage."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from nibble.adapters.trillium import TrilliumAdapter

URL = "https://trillium.example.com/vehicles"


def _sample_vehicles() -> list[dict[str, Any]]:
    return [
        {
            "id": 8819,
            "name": "1204",
            "lat": 42.76554,
            "lon": -71.09184,
            "speed": 5,
            "headingDegrees": 314,
            "lastUpdated": "2026-03-07T23:52:57Z",
            "route_id": "10729",
            "route_short_name": "16",
            "vehicleType": "bus",
        },
        {
            "id": 9001,
            "name": "2001",
            "lat": 42.1,
            "lon": -71.2,
            "speed": 0,
            "headingDegrees": None,
            "lastUpdated": "2026-03-07T23:53:00Z",
            "route_id": "10730",
        },
    ]


@pytest.fixture
def adapter() -> TrilliumAdapter:
    return TrilliumAdapter(URL)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_returns_feed(adapter: TrilliumAdapter) -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json={"data": _sample_vehicles()}))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 2


@respx.mock
@pytest.mark.asyncio
async def test_vehicle_fields_mapped(adapter: TrilliumAdapter) -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json={"data": _sample_vehicles()}))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    entity = feed.entity[0]
    assert entity.id == "8819"
    vp = entity.vehicle
    assert vp.vehicle.id == "8819"
    assert vp.vehicle.label == "1204"
    assert vp.trip.route_id == "16"  # route_short_name preferred over internal route_id
    assert abs(vp.position.latitude - 42.76554) < 1e-5
    assert abs(vp.position.longitude - (-71.09184)) < 1e-5
    assert abs(vp.position.bearing - 314.0) < 1e-5
    assert abs(vp.position.speed - 5.0) < 1e-5
    # lastUpdated parsed correctly
    assert vp.timestamp > 0


@respx.mock
@pytest.mark.asyncio
async def test_missing_optional_fields_skipped(adapter: TrilliumAdapter) -> None:
    """Vehicle without headingDegrees (null) should not set bearing field."""
    respx.get(URL).mock(return_value=httpx.Response(200, json={"data": _sample_vehicles()}))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    vp2 = feed.entity[1].vehicle
    # bearing not set - proto3 default is 0.0
    assert vp2.position.bearing == 0.0


@respx.mock
@pytest.mark.asyncio
async def test_route_short_name_preferred_over_route_id(adapter: TrilliumAdapter) -> None:
    """route_short_name should be used as route_id when present."""
    vehicles = [{"id": 1, "lat": 42.0, "lon": -71.0, "route_id": "10729", "route_short_name": "16"}]
    respx.get(URL).mock(return_value=httpx.Response(200, json={"data": vehicles}))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.trip.route_id == "16"


@respx.mock
@pytest.mark.asyncio
async def test_route_id_used_when_no_short_name(adapter: TrilliumAdapter) -> None:
    """Falls back to route_id when route_short_name is absent."""
    vehicles = [{"id": 1, "lat": 42.0, "lon": -71.0, "route_id": "10729"}]
    respx.get(URL).mock(return_value=httpx.Response(200, json={"data": vehicles}))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert feed.entity[0].vehicle.trip.route_id == "10729"


@respx.mock
@pytest.mark.asyncio
async def test_vehicle_without_id_skipped(adapter: TrilliumAdapter) -> None:
    vehicles = [{"name": "no-id", "lat": 42.0, "lon": -71.0}]
    respx.get(URL).mock(return_value=httpx.Response(200, json={"data": vehicles}))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 0


@respx.mock
@pytest.mark.asyncio
async def test_non_200_returns_none(adapter: TrilliumAdapter) -> None:
    respx.get(URL).mock(return_value=httpx.Response(503))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@respx.mock
@pytest.mark.asyncio
async def test_network_error_returns_none(adapter: TrilliumAdapter) -> None:
    respx.get(URL).mock(side_effect=httpx.ConnectError("timeout"))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@respx.mock
@pytest.mark.asyncio
async def test_invalid_json_returns_none(adapter: TrilliumAdapter) -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, content=b"not json"))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is None


@respx.mock
@pytest.mark.asyncio
async def test_malformed_last_updated_skips_timestamp(adapter: TrilliumAdapter) -> None:
    """Unparseable lastUpdated should not crash; vehicle is still returned."""
    vehicles = [{"id": 1, "lat": 42.0, "lon": -71.0, "lastUpdated": "not-a-date"}]
    respx.get(URL).mock(return_value=httpx.Response(200, json={"data": vehicles}))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 1
    # timestamp falls back to 0 (proto3 default) when parsing fails
    assert feed.entity[0].vehicle.timestamp == 0


@respx.mock
@pytest.mark.asyncio
async def test_data_as_top_level_list(adapter: TrilliumAdapter) -> None:
    """Some Trillium endpoints return the array directly without a data wrapper."""
    respx.get(URL).mock(return_value=httpx.Response(200, json=_sample_vehicles()))
    async with httpx.AsyncClient() as client:
        feed = await adapter.fetch(client)
    assert feed is not None
    assert len(feed.entity) == 2
