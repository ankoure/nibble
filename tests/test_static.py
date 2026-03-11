from __future__ import annotations

import io
import zipfile

from nibble.gtfs.static import _parse_gtfs_zip


def _make_zip(**files: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


TRIPS_HEADER = "route_id,trip_id,direction_id,shape_id\n"
STOP_TIMES_HEADER = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n"


class TestLoadTrips:
    def test_loads_trips(self):
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

    def test_strips_whitespace(self):
        data = _make_zip(
            **{
                "trips.txt": TRIPS_HEADER + " route-1 , trip-1 ,0,\n",
            }
        )
        gtfs = _parse_gtfs_zip(data)
        assert "trip-1" in gtfs.trips
        assert gtfs.trips["trip-1"].route_id == "route-1"

    def test_skips_empty_trip_id(self):
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,,0,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips == {}

    def test_skips_empty_route_id(self):
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + ",trip-1,0,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips == {}

    def test_direction_id_coercion(self):
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,trip-1,1,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips["trip-1"].direction_id == 1
        assert isinstance(gtfs.trips["trip-1"].direction_id, int)

    def test_direction_id_non_digit_becomes_none(self):
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,trip-1,x,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips["trip-1"].direction_id is None

    def test_direction_id_empty_becomes_none(self):
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,trip-1,,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips["trip-1"].direction_id is None

    def test_shape_id_empty_becomes_none(self):
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + "route-1,trip-1,0,\n"})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips["trip-1"].shape_id is None

    def test_bom_handling(self):
        # utf-8-sig codec prepends the BOM byte sequence automatically
        content = (TRIPS_HEADER + "route-1,trip-1,0,\n").encode("utf-8-sig")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("trips.txt", content)
        gtfs = _parse_gtfs_zip(buf.getvalue())
        assert "trip-1" in gtfs.trips

    def test_missing_trips_file(self):
        data = _make_zip(**{"stop_times.txt": STOP_TIMES_HEADER})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.trips == {}


class TestLoadStopTimes:
    def test_loads_stop_times(self):
        row = "trip-1,stop-1,1,12:00:00,12:00:30\n"
        data = _make_zip(**{"stop_times.txt": STOP_TIMES_HEADER + row})
        gtfs = _parse_gtfs_zip(data)
        assert "trip-1" in gtfs.stop_times
        st = gtfs.stop_times["trip-1"][0]
        assert st.stop_id == "stop-1"
        assert st.stop_sequence == 1
        assert st.arrival_time == "12:00:00"
        assert st.departure_time == "12:00:30"

    def test_skips_non_digit_stop_sequence(self):
        rows = "trip-1,stop-1,x,12:00:00,12:00:30\ntrip-1,stop-2,2,12:01:00,12:01:30\n"
        data = _make_zip(**{"stop_times.txt": STOP_TIMES_HEADER + rows})
        gtfs = _parse_gtfs_zip(data)
        # Only the valid row should be included
        assert len(gtfs.stop_times["trip-1"]) == 1
        assert gtfs.stop_times["trip-1"][0].stop_id == "stop-2"

    def test_optional_times_empty_becomes_none(self):
        data = _make_zip(
            **{
                "stop_times.txt": STOP_TIMES_HEADER + "trip-1,stop-1,1,,\n",
            }
        )
        gtfs = _parse_gtfs_zip(data)
        st = gtfs.stop_times["trip-1"][0]
        assert st.arrival_time is None
        assert st.departure_time is None

    def test_stop_times_sorted_by_sequence(self):
        rows = "trip-1,stop-c,3,,\ntrip-1,stop-a,1,,\ntrip-1,stop-b,2,,\n"
        data = _make_zip(**{"stop_times.txt": STOP_TIMES_HEADER + rows})
        gtfs = _parse_gtfs_zip(data)
        seqs = [st.stop_sequence for st in gtfs.stop_times["trip-1"]]
        assert seqs == [1, 2, 3]

    def test_missing_stop_times_file(self):
        data = _make_zip(**{"trips.txt": TRIPS_HEADER})
        gtfs = _parse_gtfs_zip(data)
        assert gtfs.stop_times == {}
