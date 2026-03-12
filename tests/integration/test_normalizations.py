"""Integration tests verifying that normalizations applied in one pipeline stage
produce correct outcomes in downstream stages.

Each test chains two components — the normalization in stage N is what enables
correct behavior in stage N+1. This is distinct from unit tests (which verify
a normalization was applied) and from pipeline tests (which verify overall flow).
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone

from google.transit import gtfs_realtime_pb2

from nibble.gtfs.static import StaticGTFS, _parse_gtfs_zip
from nibble.interpolator import interpolate
from nibble.models import Position, VehicleEvent
from nibble.normalizer.ripta import RiptaNormalizer
from nibble.poller import _parse_feed
from nibble.state import StateStore, VehicleState


def _make_zip(**files: str | bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


TRIPS_HEADER = "route_id,trip_id,direction_id,shape_id\n"
STOP_TIMES_HEADER = "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n"


def _feed_with_vehicle(
    trip_id: str,
    vehicle_id: str = "v1",
    entity_id: str = "e1",
    bearing: float = 90.0,
    vehicle_ts: int = 1704067200,
    header_ts: int = 1704067200,
) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = header_ts
    entity = feed.entity.add()
    entity.id = entity_id
    entity.vehicle.vehicle.id = vehicle_id
    entity.vehicle.trip.trip_id = trip_id
    entity.vehicle.trip.route_id = "route-1"
    entity.vehicle.position.latitude = 41.82
    entity.vehicle.position.longitude = -71.41
    entity.vehicle.position.bearing = bearing
    entity.vehicle.current_stop_sequence = 1
    entity.vehicle.timestamp = vehicle_ts
    return feed


class TestStaticGTFSNormalizationsFlowThrough:
    """Normalizations applied during _parse_gtfs_zip() must produce correct
    behavior in downstream stages (StateStore, interpolator)."""

    def test_whitespace_stripped_trip_id_resolves_confirmed(self) -> None:
        """GTFS CSV with ' trip-1 ' (padded spaces) must still allow a feed
        vehicle with trip_id='trip-1' to resolve as confidence='confirmed'.

        Proves: whitespace stripping happens before dict indexing, so the
        StateStore lookup finds the trip.
        """
        data = _make_zip(**{"trips.txt": TRIPS_HEADER + " route-1 , trip-1 ,0,\n"})
        gtfs = _parse_gtfs_zip(data)

        feed = _feed_with_vehicle("trip-1")
        events = _parse_feed(feed)

        store = StateStore()
        resolved = store.update_from_event(events["v1"], gtfs, stale_threshold_seconds=90)

        assert resolved.confidence == "confirmed"
        assert resolved.trip_id == "trip-1"

    def test_bom_prefixed_gtfs_trip_resolves_confirmed(self) -> None:
        """GTFS trips.txt encoded with utf-8-sig BOM must still allow a feed
        vehicle to resolve as confidence='confirmed'.

        Proves: the BOM is consumed by the utf-8-sig codec and not incorporated
        into the trip_id dict key.
        """
        content = (TRIPS_HEADER + "route-1,trip-1,0,\n").encode("utf-8-sig")
        data = _make_zip(**{"trips.txt": content})
        gtfs = _parse_gtfs_zip(data)

        feed = _feed_with_vehicle("trip-1")
        events = _parse_feed(feed)

        store = StateStore()
        resolved = store.update_from_event(events["v1"], gtfs, stale_threshold_seconds=90)

        assert resolved.confidence == "confirmed"
        assert resolved.trip_id == "trip-1"

    def test_unsorted_stop_times_produce_correct_interpolation(self) -> None:
        """GTFS stop_times.txt with rows in reverse sequence order must still
        produce correctly-ordered interpolated events.

        Proves: sorting during loading propagates to the interpolator so the
        intermediate stop slice is computed correctly.
        """
        data = _make_zip(
            **{
                "trips.txt": TRIPS_HEADER + "route-1,trip-1,0,\n",
                "stop_times.txt": (
                    STOP_TIMES_HEADER
                    # Intentionally reversed order in the CSV
                    + "trip-1,stop-C,3,08:10:00,08:10:00\n"
                    + "trip-1,stop-B,2,08:05:00,08:05:00\n"
                    + "trip-1,stop-A,1,08:00:00,08:00:00\n"
                ),
            }
        )
        gtfs = _parse_gtfs_zip(data)

        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        prev_state = VehicleState(
            vehicle_id="v1",
            last_seen=t0,
            confidence="confirmed",
            last_valid_trip_id="trip-1",
            last_valid_stop_id="stop-A",
            last_valid_stop_sequence=1,
            last_position=Position(latitude=41.82, longitude=-71.41),
        )
        curr_event = VehicleEvent(
            vehicle_id="v1",
            trip_id="trip-1",
            current_stop_sequence=3,
            stop_id="stop-C",
            position=Position(latitude=41.84, longitude=-71.43),
            timestamp=t1,
            confidence="confirmed",
        )

        events = interpolate(prev_state, curr_event, gtfs, max_stops=3)

        assert len(events) == 2
        assert events[0].current_stop_sequence == 2
        assert events[0].stop_id == "stop-B"
        assert events[0].provenance == "interpolated"
        assert events[1].current_stop_sequence == 3

    def test_empty_arrival_times_trigger_linear_fraction_fallback(self) -> None:
        """GTFS stop_times.txt with no arrival/departure times must still
        produce interpolated events using linear timestamp spacing.

        Proves: None arrival_time propagates to the interpolator and triggers
        the linear-fraction fallback within the schedule-aware path (rather
        than crashing or returning an empty list).
        """
        data = _make_zip(
            **{
                "trips.txt": TRIPS_HEADER + "route-1,trip-1,0,\n",
                "stop_times.txt": (
                    STOP_TIMES_HEADER
                    + "trip-1,stop-A,1,,\n"
                    + "trip-1,stop-B,2,,\n"
                    + "trip-1,stop-C,3,,\n"
                ),
            }
        )
        gtfs = _parse_gtfs_zip(data)

        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
        prev_state = VehicleState(
            vehicle_id="v1",
            last_seen=t0,
            confidence="confirmed",
            last_valid_trip_id="trip-1",
            last_valid_stop_id="stop-A",
            last_valid_stop_sequence=1,
            last_position=Position(latitude=41.82, longitude=-71.41),
        )
        curr_event = VehicleEvent(
            vehicle_id="v1",
            trip_id="trip-1",
            current_stop_sequence=3,
            stop_id="stop-C",
            position=Position(latitude=41.84, longitude=-71.43),
            timestamp=t1,
            confidence="confirmed",
        )

        events = interpolate(prev_state, curr_event, gtfs, max_stops=3)

        assert len(events) == 2
        # Intermediate event has correct stop_id (from stop_times list)
        assert events[0].stop_id == "stop-B"
        assert events[0].provenance == "interpolated"
        # Timestamps are linearly spaced between t0 and t1
        assert t0 < events[0].timestamp < events[1].timestamp <= t1


class TestFeedParsingNormalizationsFlowThrough:
    """Normalizations applied during _parse_feed() must produce correct
    behavior after state resolution via StateStore."""

    def test_zero_bearing_is_none_after_state_resolution(self, static_gtfs: StaticGTFS) -> None:
        """A feed vehicle with bearing=0.0 (due-north ambiguity) must have
        position.bearing=None after _parse_feed + StateStore.update_from_event().

        Proves: the bearing→None normalization survives state resolution
        unchanged.
        """
        feed = _feed_with_vehicle("trip-1", bearing=0.0)
        events = _parse_feed(feed)

        assert events["v1"].position.bearing is None

        store = StateStore()
        resolved = store.update_from_event(events["v1"], static_gtfs, stale_threshold_seconds=90)

        assert resolved.position.bearing is None

    def test_feed_header_timestamp_used_when_vehicle_ts_zero(self, static_gtfs: StaticGTFS) -> None:
        """A vehicle with timestamp=0 must use the feed header timestamp,
        and that timestamp must survive state resolution.

        Proves: the timestamp fallback normalization propagates correctly
        through the pipeline.
        """
        header_ts = 1704067200  # 2024-01-01 00:00:00 UTC
        feed = _feed_with_vehicle("trip-1", vehicle_ts=0, header_ts=header_ts)
        events = _parse_feed(feed)

        expected_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert events["v1"].timestamp == expected_ts

        store = StateStore()
        resolved = store.update_from_event(events["v1"], static_gtfs, stale_threshold_seconds=90)

        assert resolved.timestamp == expected_ts

    def test_vehicle_id_fallback_flows_through_to_state_store(
        self, static_gtfs: StaticGTFS
    ) -> None:
        """A feed entity with empty vehicle.id but non-empty entity.id must be
        keyed by entity.id through _parse_feed and into StateStore.

        Proves: the vehicle_id fallback normalization is consistent — the same
        key used in _parse_feed is the key used in the StateStore.
        """
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1704067200
        entity = feed.entity.add()
        entity.id = "e1"
        # Do NOT set entity.vehicle.vehicle — HasField("vehicle") will be False,
        # so _parse_feed falls back to entity.id.
        entity.vehicle.trip.trip_id = "trip-1"
        entity.vehicle.trip.route_id = "route-1"
        entity.vehicle.position.latitude = 41.82
        entity.vehicle.position.longitude = -71.41
        entity.vehicle.timestamp = 1704067200

        events = _parse_feed(feed)

        assert "e1" in events
        assert "v1" not in events

        store = StateStore()
        resolved = store.update_from_event(events["e1"], static_gtfs, stale_threshold_seconds=90)

        assert resolved.vehicle_id == "e1"
        assert resolved.confidence == "confirmed"


class TestNormalizerPipelineIntegration:
    """The RIPTA normalizer must produce trip_ids that resolve as
    confidence='confirmed' in the StateStore — not just a correctly-parsed
    string value."""

    def test_ripta_suffix_strip_produces_confirmed_confidence(
        self, static_gtfs: StaticGTFS
    ) -> None:
        """After RIPTA normalization strips the date suffix, the StateStore must
        resolve the vehicle as confidence='confirmed' because the normalized
        trip_id is found in static GTFS.

        Proves: trip_id normalization enables GTFS lookup, not just a correct
        string value in the parsed event.
        """
        normalizer = RiptaNormalizer()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1704067200
        entity = feed.entity.add()
        entity.id = "e1"
        entity.vehicle.vehicle.id = "v1"
        entity.vehicle.trip.trip_id = "trip-1_20240101"  # date-suffixed, not in static GTFS
        entity.vehicle.trip.route_id = "route-1"
        entity.vehicle.position.latitude = 41.82
        entity.vehicle.position.longitude = -71.41
        entity.vehicle.current_stop_sequence = 1
        entity.vehicle.timestamp = 1704067200

        normalized = normalizer.normalize(feed, static_gtfs)
        events = _parse_feed(normalized)

        assert events["v1"].trip_id == "trip-1"

        store = StateStore()
        resolved = store.update_from_event(events["v1"], static_gtfs, stale_threshold_seconds=90)

        assert resolved.confidence == "confirmed"
        assert resolved.trip_id == "trip-1"
