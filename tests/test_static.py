from __future__ import annotations

import io
import zipfile

from nibble.gtfs.static import (
    _angle_difference,
    _parse_gtfs_zip,
    _shape_bearing_at_projection,
    infer_stop_from_position,
    infer_trip_from_position,
)
from nibble.gtfs.static import StaticGTFS
from nibble.models import StopTime, Trip


def _make_zip(**files: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


TRIPS_HEADER = "route_id,trip_id,direction_id,shape_id\n"
STOP_TIMES_HEADER = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n"


class TestLoadTrips:
    def test_loads_trips(self) -> None:
        data = _make_zip(
            **{
                "trips.txt": TRIPS_HEADER + "route-1,trip-1,0,shape-1\n",
            }
        )
        gtfs = _parse_gtfs_zip(data)
        assert "trip-1" in gtfs.trips
        trip = gtfs.trips["trip-1"]
        assert trip.route_id == "route-1"
        assert trip.direction_id == 0
        assert trip.shape_id == "shape-1"

    def test_strips_whitespace(self) -> None:
        data = _make_zip(
            **{
                "trips.txt": TRIPS_HEADER + " route-1 , trip-1 ,0,\n",
            }
        )
        gtfs = _parse_gtfs_zip(data)
        assert "trip-1" in gtfs.trips
        assert gtfs.trips["trip-1"].route_id == "route-1"

    def test_skips_empty_trip_id(self) -> None:
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,,0,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips == {}

    def test_skips_empty_route_id(self) -> None:
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + ",trip-1,0,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips == {}

    def test_direction_id_coercion(self) -> None:
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,trip-1,1,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips["trip-1"].direction_id == 1
        assert isinstance(gtfs.trips["trip-1"].direction_id, int)

    def test_direction_id_non_digit_becomes_none(self) -> None:
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,trip-1,x,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips["trip-1"].direction_id is None

    def test_direction_id_empty_becomes_none(self) -> None:
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,trip-1,,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips["trip-1"].direction_id is None

    def test_shape_id_empty_becomes_none(self) -> None:
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,trip-1,0,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips["trip-1"].shape_id is None

    def test_bom_handling(self) -> None:
        # utf-8-sig codec prepends the BOM byte sequence automatically
        content = (TRIPS_HEADER + "route-1,trip-1,0,\n").encode("utf-8-sig")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("trips.txt", content)
        gtfs = _parse_gtfs_zip(buf.getvalue())
        assert "trip-1" in gtfs.trips

    def test_missing_trips_file(self) -> None:
        data = _make_zip(**{"stop_times.txt": STOP_TIMES_HEADER})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips == {}


class TestLoadStopTimes:
    def test_loads_stop_times(self) -> None:
        row = "trip-1,stop-1,1,12:00:00,12:00:30\n"
        data = _make_zip(**{"stop_times.txt": STOP_TIMES_HEADER + row})
        gtfs = _parse_gtfs_zip(data)
        assert "trip-1" in gtfs.stop_times
        st = gtfs.stop_times["trip-1"][0]
        assert st.stop_id == "stop-1"
        assert st.stop_sequence == 1
        assert st.arrival_time == "12:00:00"
        assert st.departure_time == "12:00:30"

    def test_skips_non_digit_stop_sequence(self) -> None:
        rows = "trip-1,stop-1,x,12:00:00,12:00:30\ntrip-1,stop-2,2,12:01:00,12:01:30\n"
        data = _make_zip(**{"stop_times.txt": STOP_TIMES_HEADER + rows})
        gtfs = _parse_gtfs_zip(data)
        # Only the valid row should be included
        assert len(gtfs.stop_times["trip-1"]) == 1
        assert gtfs.stop_times["trip-1"][0].stop_id == "stop-2"

    def test_optional_times_empty_becomes_none(self) -> None:
        data = _make_zip(
            **{
                "stop_times.txt": STOP_TIMES_HEADER + "trip-1,stop-1,1,,\n",
            }
        )
        gtfs = _parse_gtfs_zip(data)
        st = gtfs.stop_times["trip-1"][0]
        assert st.arrival_time is None
        assert st.departure_time is None

    def test_stop_times_sorted_by_sequence(self) -> None:
        rows = "trip-1,stop-c,3,,\ntrip-1,stop-a,1,,\ntrip-1,stop-b,2,,\n"
        data = _make_zip(**{"stop_times.txt": STOP_TIMES_HEADER + rows})
        gtfs = _parse_gtfs_zip(data)
        seqs = [st.stop_sequence for st in gtfs.stop_times["trip-1"]]
        assert seqs == [1, 2, 3]

    def test_missing_stop_times_file(self) -> None:
        data = _make_zip(**{"trips.txt": TRIPS_HEADER})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.stop_times == {}


class TestAngleDifference:
    def test_same_bearing(self) -> None:
        assert _angle_difference(90.0, 90.0) == 0.0

    def test_opposite_bearings(self) -> None:
        assert _angle_difference(0.0, 180.0) == 180.0

    def test_wraparound(self) -> None:
        # 350° and 10° are 20° apart, not 340°
        assert abs(_angle_difference(350.0, 10.0) - 20.0) < 0.001

    def test_right_angle(self) -> None:
        assert abs(_angle_difference(0.0, 90.0) - 90.0) < 0.001

    def test_symmetric(self) -> None:
        assert _angle_difference(30.0, 200.0) == _angle_difference(200.0, 30.0)


class TestShapeBearingAtProjection:
    def test_northbound_segment(self) -> None:
        # Shape runs due north; vehicle is on that segment
        shape = [(42.00, -71.0), (42.10, -71.0)]
        bearing = _shape_bearing_at_projection(42.05, -71.0, shape)
        assert bearing is not None
        assert abs(bearing - 0.0) < 1.0  # ~north

    def test_eastbound_segment(self) -> None:
        shape = [(42.0, -71.10), (42.0, -71.00)]
        bearing = _shape_bearing_at_projection(42.0, -71.05, shape)
        assert bearing is not None
        assert abs(bearing - 90.0) < 1.0  # ~east

    def test_southbound_segment(self) -> None:
        shape = [(42.10, -71.0), (42.00, -71.0)]
        bearing = _shape_bearing_at_projection(42.05, -71.0, shape)
        assert bearing is not None
        assert abs(bearing - 180.0) < 1.0  # ~south

    def test_returns_none_for_single_point(self) -> None:
        assert _shape_bearing_at_projection(42.0, -71.0, [(42.0, -71.0)]) is None

    def test_picks_nearest_segment(self) -> None:
        # Shape: northbound then turns eastbound; vehicle is clearly near the eastbound segment
        shape = [(42.00, -71.0), (42.10, -71.0), (42.10, -70.9)]
        # Vehicle near the eastbound leg - expected bearing ~90°
        bearing = _shape_bearing_at_projection(42.10, -70.95, shape)
        assert bearing is not None
        assert abs(bearing - 90.0) < 5.0


def _gtfs_with_two_trips(
    route_id: str = "route-1",
    shape_northbound: list[tuple[float, float]] | None = None,
    shape_southbound: list[tuple[float, float]] | None = None,
) -> StaticGTFS:
    """Build a StaticGTFS with two trips on the same route going opposite directions."""
    if shape_northbound is None:
        shape_northbound = [(42.00, -71.0), (42.10, -71.0)]
    if shape_southbound is None:
        shape_southbound = [(42.10, -71.0), (42.00, -71.0)]
    gtfs = StaticGTFS()
    gtfs.trips["trip-nb"] = Trip(
        trip_id="trip-nb", route_id=route_id, direction_id=0, shape_id="shape-nb"
    )
    gtfs.trips["trip-sb"] = Trip(
        trip_id="trip-sb", route_id=route_id, direction_id=1, shape_id="shape-sb"
    )
    gtfs.shapes["shape-nb"] = shape_northbound
    gtfs.shapes["shape-sb"] = shape_southbound
    gtfs.route_trips[route_id] = ["trip-nb", "trip-sb"]
    return gtfs


class TestInferTripFromPositionBearing:
    def test_bearing_selects_northbound_trip(self) -> None:
        gtfs = _gtfs_with_two_trips()
        # Vehicle midway on route, heading north (~0°)
        result = infer_trip_from_position(42.05, -71.0, "route-1", gtfs, bearing=0.0)
        assert result == "trip-nb"

    def test_bearing_selects_southbound_trip(self) -> None:
        gtfs = _gtfs_with_two_trips()
        # Vehicle midway on route, heading south (~180°)
        result = infer_trip_from_position(42.05, -71.0, "route-1", gtfs, bearing=180.0)
        assert result == "trip-sb"

    def test_no_bearing_returns_closest_by_distance(self) -> None:
        # Without bearing, both trips are equidistant (same position on mirrored shapes)
        # - just verify it returns one of them without error
        gtfs = _gtfs_with_two_trips()
        result = infer_trip_from_position(42.05, -71.0, "route-1", gtfs, bearing=None)
        assert result in ("trip-nb", "trip-sb")

    def test_bearing_filter_falls_back_when_all_filtered(self) -> None:
        # Perpendicular bearing (90°) filters both trips - should still return a result
        gtfs = _gtfs_with_two_trips()
        result = infer_trip_from_position(42.05, -71.0, "route-1", gtfs, bearing=90.0)
        assert result in ("trip-nb", "trip-sb")

    def test_bearing_filter_passes_trip_without_shape(self) -> None:
        # A trip with no shape should survive the bearing filter (not be dropped)
        gtfs = _gtfs_with_two_trips()
        gtfs.trips["trip-no-shape"] = Trip(
            trip_id="trip-no-shape", route_id="route-1", shape_id=None
        )
        gtfs.route_trips["route-1"].append("trip-no-shape")
        # All trips pass the filter (no-shape trips are always kept); result is one of the valid ones
        result = infer_trip_from_position(42.05, -71.0, "route-1", gtfs, bearing=0.0)
        assert result is not None


def _gtfs_with_shape_and_stops() -> StaticGTFS:
    """Build a minimal StaticGTFS suitable for infer_stop_from_position testing.

    Shape: two points running north along lon=-71.
    Trip has two stops with shape_dist_traveled values.
    """
    gtfs = StaticGTFS()
    gtfs.trips["trip-1"] = Trip(trip_id="trip-1", route_id="route-1", shape_id="shape-1")
    gtfs.shapes["shape-1"] = [(42.00, -71.0), (42.10, -71.0)]
    # Stop A at 0m (42.00, -71.0), stop B at ~11km (42.10, -71.0)
    gtfs.stops["stop-A"] = (42.00, -71.0)
    gtfs.stops["stop-B"] = (42.10, -71.0)
    shape_len = 11131.9  # approximate metres between the two shape points
    gtfs.stop_times["trip-1"] = [
        StopTime(
            trip_id="trip-1",
            stop_id="stop-A",
            stop_sequence=1,
            arrival_time="12:00:00",
            departure_time="12:00:30",
            shape_dist_traveled=0.0,
        ),
        StopTime(
            trip_id="trip-1",
            stop_id="stop-B",
            stop_sequence=2,
            arrival_time="12:10:00",
            departure_time="12:10:30",
            shape_dist_traveled=shape_len,
        ),
    ]
    return gtfs


class TestInferStopFromPosition:
    def test_vehicle_at_first_stop_is_stopped_at(self) -> None:
        gtfs = _gtfs_with_shape_and_stops()
        stop_id, seq, status = infer_stop_from_position(42.00, -71.0, "trip-1", gtfs)
        assert stop_id == "stop-A"
        assert seq == 1
        assert status == "STOPPED_AT"

    def test_vehicle_at_second_stop_is_stopped_at(self) -> None:
        gtfs = _gtfs_with_shape_and_stops()
        stop_id, seq, status = infer_stop_from_position(42.10, -71.0, "trip-1", gtfs)
        assert stop_id == "stop-B"
        assert seq == 2
        assert status == "STOPPED_AT"

    def test_vehicle_midway_is_in_transit_to_second_stop(self) -> None:
        gtfs = _gtfs_with_shape_and_stops()
        # At ~42.05, well past stop-A but not yet stop-B
        stop_id, seq, status = infer_stop_from_position(42.05, -71.0, "trip-1", gtfs)
        assert stop_id == "stop-B"
        assert seq == 2
        assert status == "IN_TRANSIT_TO"

    def test_vehicle_past_last_stop_returns_last_stop(self) -> None:
        gtfs = _gtfs_with_shape_and_stops()
        # Vehicle past the end of the shape - projects onto the last vertex, which is
        # within stopped threshold of stop-B, so it appears STOPPED_AT the last stop.
        stop_id, seq, status = infer_stop_from_position(42.15, -71.0, "trip-1", gtfs)
        assert stop_id == "stop-B"
        assert seq == 2
        assert status == "STOPPED_AT"

    def test_unknown_trip_returns_nones(self) -> None:
        gtfs = _gtfs_with_shape_and_stops()
        stop_id, seq, status = infer_stop_from_position(42.05, -71.0, "no-such-trip", gtfs)
        assert stop_id is None
        assert seq is None
        assert status == "IN_TRANSIT_TO"

    def test_trip_without_shape_returns_nones(self) -> None:
        gtfs = _gtfs_with_shape_and_stops()
        gtfs.trips["trip-no-shape"] = Trip(trip_id="trip-no-shape", route_id="r", shape_id=None)
        gtfs.stop_times["trip-no-shape"] = gtfs.stop_times["trip-1"]
        stop_id, seq, status = infer_stop_from_position(42.05, -71.0, "trip-no-shape", gtfs)
        assert stop_id is None
        assert seq is None

    def test_no_stop_times_with_shape_dist_returns_nones(self) -> None:
        gtfs = _gtfs_with_shape_and_stops()
        # Strip shape_dist_traveled from all stop times
        for st in gtfs.stop_times["trip-1"]:
            st.shape_dist_traveled = None
        # _fill_shape_dist_traveled would normally fill these, but we test the raw guard
        stop_id, seq, status = infer_stop_from_position(42.05, -71.0, "trip-1", gtfs)
        # Without shape_dist_traveled the function returns nones
        assert stop_id is None
