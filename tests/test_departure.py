"""Tests for nibble.departure.detect_departures."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from nibble.departure import detect_departures
from nibble.models import Position, VehicleEvent


def _vehicle(trip_id: str, timestamp: datetime, vehicle_id: str = "v1") -> VehicleEvent:
    return VehicleEvent(
        vehicle_id=vehicle_id,
        position=Position(latitude=0.0, longitude=0.0),
        timestamp=timestamp,
        trip_id=trip_id,
    )


class TestDetectDepartures:
    def test_head_unchanged_yields_no_departure(self) -> None:
        feed_time = datetime(2026, 4, 13, tzinfo=timezone.utc)
        result = detect_departures(
            {"T1": "S1"},
            {"T1": "S1"},
            {"T1": _vehicle("T1", feed_time)},
            feed_time,
            90,
        )
        assert result == set()

    def test_head_changed_fresh_vehicle_detected(self) -> None:
        feed_time = datetime(2026, 4, 13, tzinfo=timezone.utc)
        result = detect_departures(
            {"T1": "S1"},
            {"T1": "S2"},
            {"T1": _vehicle("T1", feed_time)},
            feed_time,
            90,
        )
        assert result == {"T1"}

    def test_stalled_vehicle_suppresses_departure(
        self, caplog: "__import__('pytest').LogCaptureFixture"
    ) -> None:
        feed_time = datetime(2026, 4, 13, tzinfo=timezone.utc)
        stale_ts = feed_time - timedelta(seconds=120)
        with caplog.at_level(logging.INFO, logger="nibble.departure"):
            result = detect_departures(
                {"T1": "S1"},
                {"T1": "S2"},
                {"T1": _vehicle("T1", stale_ts)},
                feed_time,
                90,
            )
        assert result == set()
        assert any("Suppressed inferred departure" in r.message for r in caplog.records)

    def test_trip_without_matching_vehicle_ignored(self) -> None:
        feed_time = datetime(2026, 4, 13, tzinfo=timezone.utc)
        result = detect_departures(
            {"T1": "S1"},
            {"T1": "S2"},
            {},
            feed_time,
            90,
        )
        assert result == set()

    def test_new_trip_without_prev_head_ignored(self) -> None:
        feed_time = datetime(2026, 4, 13, tzinfo=timezone.utc)
        result = detect_departures(
            {},
            {"T1": "S1"},
            {"T1": _vehicle("T1", feed_time)},
            feed_time,
            90,
        )
        assert result == set()

    def test_lag_equal_to_threshold_is_allowed(self) -> None:
        feed_time = datetime(2026, 4, 13, tzinfo=timezone.utc)
        boundary_ts = feed_time - timedelta(seconds=90)
        result = detect_departures(
            {"T1": "S1"},
            {"T1": "S2"},
            {"T1": _vehicle("T1", boundary_ts)},
            feed_time,
            90,
        )
        assert result == {"T1"}
