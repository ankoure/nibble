"""Integration tests for the full GTFS-RT → SSE event pipeline.

Tests wire together _parse_feed, StateStore, reconcile, and interpolate
using real data objects — no HTTP mocking required.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from google.transit import gtfs_realtime_pb2

from nibble.config import Settings
from nibble.gtfs.static import StaticGTFS
from nibble.models import VehicleEvent, Position
from nibble.normalizer.ripta import RiptaNormalizer
from nibble.poller import _parse_feed
from nibble.reconciler import reconcile
from nibble.state import StateStore


def _settings(**kwargs: Any) -> Settings:
    defaults: dict[str, Any] = dict(
        gtfs_rt_url="http://example.com/rt",
        gtfs_static_url="http://example.com/static.zip",
        stale_vehicle_threshold_seconds=90,
        max_interpolation_stops=3,
    )
    defaults.update(kwargs)
    return Settings(_env_file=None, **defaults)


def _ts(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, offset_seconds, tzinfo=timezone.utc)


class TestParseFeedIntegration:
    def test_parse_feed_returns_vehicles_keyed_by_id(
        self, feed_message: gtfs_realtime_pb2.FeedMessage
    ) -> None:
        result = _parse_feed(feed_message)
        assert "v1" in result
        assert "v2" in result

    def test_parse_feed_extracts_trip_id(self, feed_message: gtfs_realtime_pb2.FeedMessage) -> None:
        result = _parse_feed(feed_message)
        assert result["v1"].trip_id == "trip-1"
        assert result["v2"].trip_id == "trip-2"

    def test_parse_feed_extracts_position(
        self, feed_message: gtfs_realtime_pb2.FeedMessage
    ) -> None:
        result = _parse_feed(feed_message)
        assert abs(result["v1"].position.latitude - 41.82) < 0.001
        assert abs(result["v1"].position.longitude - -71.41) < 0.001

    def test_parse_feed_extracts_stop_sequence(
        self, feed_message: gtfs_realtime_pb2.FeedMessage
    ) -> None:
        result = _parse_feed(feed_message)
        assert result["v1"].current_stop_sequence == 1


class TestReconcileIntegration:
    def test_first_poll_emits_reset_with_all_vehicles(
        self, feed_message: gtfs_realtime_pb2.FeedMessage, static_gtfs: StaticGTFS
    ) -> None:
        curr = _parse_feed(feed_message)
        store = StateStore()
        config = _settings()
        events, _ = reconcile({}, curr, store, static_gtfs, config)
        assert len(events) == 1
        assert events[0].event_type == "reset"
        ids = {d["id"] for d in events[0].data}
        assert ids == {"v1", "v2"}

    def test_changed_stop_sequence_emits_update(
        self, feed_message: gtfs_realtime_pb2.FeedMessage, static_gtfs: StaticGTFS
    ) -> None:
        config = _settings()
        store = StateStore()

        # First poll: establish state
        curr = _parse_feed(feed_message)
        reconcile({}, curr, store, static_gtfs, config)

        # Second poll: advance v1 to stop sequence 3 (skipping seq 2 for interpolation)
        feed2 = gtfs_realtime_pb2.FeedMessage()
        feed2.header.gtfs_realtime_version = "2.0"
        feed2.header.timestamp = 1704067215

        e1 = feed2.entity.add()
        e1.id = "e1"
        e1.vehicle.vehicle.id = "v1"
        e1.vehicle.trip.trip_id = "trip-1"
        e1.vehicle.trip.route_id = "route-1"
        e1.vehicle.position.latitude = 41.82
        e1.vehicle.position.longitude = -71.41
        e1.vehicle.current_stop_sequence = 2
        e1.vehicle.timestamp = 1704067215

        curr2 = _parse_feed(feed2)
        events, _ = reconcile(curr, curr2, store, static_gtfs, config)
        update_events = [e for e in events if e.event_type == "update"]
        assert update_events, "Expected at least one update event"

    def test_disappeared_vehicle_emits_remove(
        self, feed_message: gtfs_realtime_pb2.FeedMessage, static_gtfs: StaticGTFS
    ) -> None:
        config = _settings()
        store = StateStore()

        curr = _parse_feed(feed_message)
        reconcile({}, curr, store, static_gtfs, config)

        # Second poll: v2 disappears
        feed2 = gtfs_realtime_pb2.FeedMessage()
        feed2.header.gtfs_realtime_version = "2.0"
        feed2.header.timestamp = 1704067215
        e1 = feed2.entity.add()
        e1.id = "e1"
        e1.vehicle.vehicle.id = "v1"
        e1.vehicle.trip.trip_id = "trip-1"
        e1.vehicle.trip.route_id = "route-1"
        e1.vehicle.position.latitude = 41.82
        e1.vehicle.position.longitude = -71.41
        e1.vehicle.current_stop_sequence = 1
        e1.vehicle.timestamp = 1704067215

        curr2 = _parse_feed(feed2)
        events, _ = reconcile(curr, curr2, store, static_gtfs, config)
        remove_events = [e for e in events if e.event_type == "remove"]
        assert remove_events
        removed_ids = {e.data["id"] for e in remove_events}
        assert "v2" in removed_ids

    def test_stale_vehicle_emits_remove(self, static_gtfs: StaticGTFS) -> None:
        config = _settings(stale_vehicle_threshold_seconds=5)
        store = StateStore()

        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)  # 60s > 5s threshold

        prev = {
            "v1": VehicleEvent(
                vehicle_id="v1",
                trip_id="trip-1",
                position=Position(latitude=41.82, longitude=-71.41),
                timestamp=t0,
            )
        }
        reconcile({}, prev, store, static_gtfs, config)

        curr = {
            "v1": VehicleEvent(
                vehicle_id="v1",
                trip_id=None,  # lost trip_id
                position=Position(latitude=41.82, longitude=-71.41),
                timestamp=t1,
            )
        }
        events, _ = reconcile(prev, curr, store, static_gtfs, config)
        remove_events = [e for e in events if e.event_type == "remove"]
        assert remove_events


class TestInterpolationIntegration:
    def test_stop_gap_produces_interpolated_events(self, static_gtfs: StaticGTFS) -> None:
        """Jumping from seq 1 to seq 3 on the same trip with real StaticGTFS stop times
        should produce intermediate synthetic events via schedule-aware interpolation."""
        from nibble.interpolator import interpolate
        from nibble.state import VehicleState

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

        events = interpolate(prev_state, curr_event, static_gtfs, max_stops=3)

        # Should return 2 events: synthetic for seq 2, observed for seq 3
        assert len(events) == 2

        # Intermediate event uses stop-B's scheduled time
        assert events[0].current_stop_sequence == 2
        assert events[0].stop_id == "stop-B"
        assert events[0].provenance == "interpolated"
        assert events[0].confidence == "inferred"

        # Final event uses the observed current stop
        assert events[1].current_stop_sequence == 3
        assert events[1].stop_id == "stop-C"
        assert events[1].provenance == "observed"

    def test_interpolation_timestamps_are_ordered(self, static_gtfs: StaticGTFS) -> None:
        """Interpolated events should have timestamps between prev and curr."""
        from nibble.interpolator import interpolate
        from nibble.state import VehicleState

        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)

        prev_state = VehicleState(
            vehicle_id="v1",
            last_seen=t0,
            confidence="confirmed",
            last_valid_trip_id="trip-1",
            last_valid_stop_sequence=1,
            last_position=Position(latitude=41.82, longitude=-71.41),
        )
        curr_event = VehicleEvent(
            vehicle_id="v1",
            trip_id="trip-1",
            current_stop_sequence=3,
            position=Position(latitude=41.84, longitude=-71.43),
            timestamp=t1,
        )

        events = interpolate(prev_state, curr_event, static_gtfs, max_stops=3)

        assert len(events) == 2
        assert t0 < events[0].timestamp <= events[1].timestamp <= t1


class TestNormalizerIntegration:
    def test_ripta_normalizer_strips_suffix_before_parse(self, static_gtfs: StaticGTFS) -> None:
        """RIPTA feed with date-suffixed trip_id should be normalized to base trip_id."""
        normalizer = RiptaNormalizer()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1704067200

        entity = feed.entity.add()
        entity.id = "e1"
        entity.vehicle.vehicle.id = "v1"
        entity.vehicle.trip.trip_id = "trip-1_20240101"  # date-suffixed, not in static
        entity.vehicle.trip.route_id = "route-1"
        entity.vehicle.position.latitude = 41.82
        entity.vehicle.position.longitude = -71.41
        entity.vehicle.current_stop_sequence = 1
        entity.vehicle.timestamp = 1704067200

        normalized = normalizer.normalize(feed, static_gtfs)
        result = _parse_feed(normalized)

        assert "v1" in result
        assert result["v1"].trip_id == "trip-1"
