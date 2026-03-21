from __future__ import annotations

from datetime import datetime, timezone

from nibble.emitter import to_mbta_v3
from nibble.models import Position, VehicleEvent


def _event(**kwargs) -> VehicleEvent:
    defaults = dict(
        vehicle_id="v1",
        trip_id="trip-1",
        route_id="route-1",
        stop_id="stop-A",
        current_stop_sequence=3,
        current_status="STOPPED_AT",
        direction_id=0,
        label="101",
        position=Position(latitude=41.82, longitude=-71.41, bearing=90.0, speed=12.5),
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        provenance="observed",
        confidence="confirmed",
    )
    defaults.update(kwargs)
    return VehicleEvent(**defaults)


class TestToMbtaV3:
    def test_top_level_structure(self) -> None:
        result = to_mbta_v3(_event())
        assert result["id"] == "v1"
        assert result["type"] == "vehicle"
        assert "attributes" in result
        assert "relationships" in result

    def test_attributes_all_fields(self) -> None:
        event = _event()
        attrs = to_mbta_v3(event)["attributes"]
        assert attrs["current_status"] == "STOPPED_AT"
        assert attrs["current_stop_sequence"] == 3
        assert attrs["direction_id"] == 0
        assert attrs["label"] == "101"
        assert attrs["latitude"] == 41.82
        assert attrs["longitude"] == -71.41
        assert attrs["bearing"] == 90.0
        assert attrs["speed"] == 12.5
        assert attrs["updated_at"] == "2024-01-01T12:00:00+00:00"
        assert attrs["occupancy_status"] is None
        assert attrs["carriages"] == []
        assert attrs["provenance"] == "observed"
        assert attrs["confidence"] == "confirmed"

    def test_relationships_with_all_ids(self) -> None:
        rels = to_mbta_v3(_event())["relationships"]
        assert rels["trip"] == {"data": {"id": "trip-1", "type": "trip"}}
        assert rels["route"] == {"data": {"id": "route-1", "type": "route"}}
        assert rels["stop"] == {"data": {"id": "stop-A", "type": "stop"}}

    def test_trip_id_none_gives_null_data(self) -> None:
        rels = to_mbta_v3(_event(trip_id=None))["relationships"]
        assert rels["trip"] == {"data": None}

    def test_route_id_none_gives_null_data(self) -> None:
        rels = to_mbta_v3(_event(route_id=None))["relationships"]
        assert rels["route"] == {"data": None}

    def test_stop_id_none_gives_null_data(self) -> None:
        rels = to_mbta_v3(_event(stop_id=None))["relationships"]
        assert rels["stop"] == {"data": None}

    def test_id_matches_vehicle_id(self) -> None:
        result = to_mbta_v3(_event(vehicle_id="bus-99"))
        assert result["id"] == "bus-99"
