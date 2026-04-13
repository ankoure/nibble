"""Tests for nibble.poller.poll_loop.

Strategy: patch asyncio.sleep to raise CancelledError after each cycle so the
infinite loop runs exactly one iteration before stopping cleanly. CancelledError
is a BaseException - it bypasses the `except Exception` error handler inside the
loop and propagates out.
"""

from __future__ import annotations

import asyncio
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nibble.config import Settings
from nibble.gtfs.static import StaticGTFS
from nibble.poller import poll_loop
from nibble.protos import gtfs_realtime_pb2


def _settings(**kwargs: Any) -> Settings:
    defaults: dict[str, Any] = dict(
        gtfs_rt_url="http://example.com/rt",
        gtfs_static_url="http://example.com/static.zip",
        poll_interval_seconds=1,
    )
    defaults.update(kwargs)
    return Settings(_env_file=None, **defaults)


def _feed_with_vehicle(vehicle_id: str = "v1") -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1704067200
    e = feed.entity.add()
    e.id = vehicle_id
    e.vehicle.vehicle.id = vehicle_id
    e.vehicle.trip.trip_id = "trip-1"
    e.vehicle.trip.route_id = "route-1"
    e.vehicle.position.latitude = 41.82
    e.vehicle.position.longitude = -71.41
    e.vehicle.timestamp = 1704067200
    return feed


def _mock_broadcaster() -> MagicMock:
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    broadcaster.vehicle_snapshot = {}
    return broadcaster


async def _run_one_cycle(
    config: Settings, gtfs: StaticGTFS, broadcaster: Any, adapter: Any, **kw: Any
) -> None:
    """Run poll_loop for exactly one iteration by stopping at the sleep call."""
    with patch("nibble.poller.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_sleep.side_effect = asyncio.CancelledError()
        with pytest.raises(asyncio.CancelledError):
            await poll_loop(config, gtfs, broadcaster, adapter=adapter, **kw)
    return mock_sleep


class TestPollLoopSuccessfulCycle:
    async def test_adapter_fetch_is_called(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = _feed_with_vehicle()
        broadcaster = _mock_broadcaster()
        config = _settings()

        await _run_one_cycle(config, StaticGTFS(), broadcaster, adapter)

        adapter.fetch.assert_called_once()

    async def test_broadcast_called_with_sse_events(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = _feed_with_vehicle()
        broadcaster = _mock_broadcaster()

        await _run_one_cycle(_settings(), StaticGTFS(), broadcaster, adapter)

        broadcaster.broadcast.assert_called_once()

    async def test_vehicle_snapshot_set_on_broadcaster(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = _feed_with_vehicle("v1")
        broadcaster = _mock_broadcaster()

        await _run_one_cycle(_settings(), StaticGTFS(), broadcaster, adapter)

        assert broadcaster.vehicle_snapshot is not None

    async def test_last_poll_time_set(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = _feed_with_vehicle()
        broadcaster = _mock_broadcaster()

        await _run_one_cycle(_settings(), StaticGTFS(), broadcaster, adapter)

        assert broadcaster.last_poll_time is not None
        assert broadcaster.last_poll_time.tzinfo == timezone.utc

    async def test_sleep_called_with_poll_interval(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = _feed_with_vehicle()
        broadcaster = _mock_broadcaster()
        config = _settings(poll_interval_seconds=30)

        with patch("nibble.poller.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError):
                await poll_loop(config, StaticGTFS(), broadcaster, adapter=adapter)

        mock_sleep.assert_called_once_with(30)


class TestPollLoopFeedNone:
    async def test_broadcast_not_called_when_feed_is_none(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = None
        broadcaster = _mock_broadcaster()

        await _run_one_cycle(_settings(), StaticGTFS(), broadcaster, adapter)

        broadcaster.broadcast.assert_not_called()

    async def test_sleep_still_called_when_feed_is_none(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = None
        broadcaster = _mock_broadcaster()

        with patch("nibble.poller.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError):
                await poll_loop(_settings(), StaticGTFS(), broadcaster, adapter=adapter)

        mock_sleep.assert_called_once()


class TestPollLoopErrorHandling:
    async def test_adapter_exception_is_caught_loop_continues(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.side_effect = Exception("network failure")
        broadcaster = _mock_broadcaster()

        # Loop catches exception, then sleeps (CancelledError stops it)
        with patch("nibble.poller.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError):
                await poll_loop(_settings(), StaticGTFS(), broadcaster, adapter=adapter)

        mock_sleep.assert_called_once()
        broadcaster.broadcast.assert_not_called()

    async def test_adapter_exception_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        adapter = AsyncMock()
        adapter.fetch.side_effect = RuntimeError("something bad")
        broadcaster = _mock_broadcaster()

        with caplog.at_level(logging.ERROR, logger="nibble.poller"):
            with patch("nibble.poller.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = asyncio.CancelledError()
                with pytest.raises(asyncio.CancelledError):
                    await poll_loop(_settings(), StaticGTFS(), broadcaster, adapter=adapter)

        assert any("Unexpected error" in r.message for r in caplog.records)


class TestPollLoopOnSnapshot:
    async def test_on_snapshot_called_with_current_vehicles(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = _feed_with_vehicle("v1")
        broadcaster = _mock_broadcaster()
        on_snapshot = AsyncMock()

        await _run_one_cycle(
            _settings(), StaticGTFS(), broadcaster, adapter, on_snapshot=on_snapshot
        )

        on_snapshot.assert_called_once()
        snapshot_arg = on_snapshot.call_args[0][0]
        assert "v1" in snapshot_arg

    async def test_on_snapshot_error_does_not_abort_loop(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.return_value = _feed_with_vehicle()
        broadcaster = _mock_broadcaster()
        on_snapshot = AsyncMock(side_effect=RuntimeError("callback failed"))

        # Should not raise - error is caught internally
        with patch("nibble.poller.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError):
                await poll_loop(
                    _settings(), StaticGTFS(), broadcaster, adapter=adapter, on_snapshot=on_snapshot
                )

        mock_sleep.assert_called_once()


def _feed_with_trip_update(
    vehicle_id: str,
    trip_id: str,
    head_stop_id: str,
    vehicle_ts: int = 1704067200,
    feed_ts: int = 1704067200,
    current_status: int = 2,
) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = feed_ts
    v = feed.entity.add()
    v.id = vehicle_id
    v.vehicle.vehicle.id = vehicle_id
    v.vehicle.trip.trip_id = trip_id
    v.vehicle.trip.route_id = "route-1"
    v.vehicle.position.latitude = 40.0
    v.vehicle.position.longitude = -73.0
    v.vehicle.timestamp = vehicle_ts
    v.vehicle.current_status = current_status
    tu = feed.entity.add()
    tu.id = f"tu-{trip_id}"
    tu.trip_update.trip.trip_id = trip_id
    stu = tu.trip_update.stop_time_update.add()
    stu.stop_id = head_stop_id
    stu.stop_sequence = 1
    stu2 = tu.trip_update.stop_time_update.add()
    stu2.stop_id = f"{head_stop_id}-next"
    stu2.stop_sequence = 2
    return feed


class TestPollLoopDepartureInference:
    async def test_head_change_synthesizes_in_transit_to(self) -> None:
        adapter = AsyncMock()
        adapter.fetch.side_effect = [
            _feed_with_trip_update("v1", "trip-1", "stopA", current_status=1),
            _feed_with_trip_update("v1", "trip-1", "stopB", current_status=1),
        ]
        broadcaster = _mock_broadcaster()
        config = _settings(infer_in_transit_from_trip_updates=True)

        call_count = {"n": 0}

        async def sleep_side_effect(_: float) -> None:
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise asyncio.CancelledError()

        with patch("nibble.poller.asyncio.sleep", new=sleep_side_effect):
            with pytest.raises(asyncio.CancelledError):
                await poll_loop(config, StaticGTFS(), broadcaster, adapter=adapter)

        # Second broadcast should include an update with IN_TRANSIT_TO
        second_call_events = broadcaster.broadcast.call_args_list[1][0][0]
        statuses = [
            ev.data.get("attributes", {}).get("current_status")
            for ev in second_call_events
            if ev.event_type == "update" and isinstance(ev.data, dict)
        ]
        assert "IN_TRANSIT_TO" in statuses

    async def test_stalled_vehicle_suppresses_inference(self) -> None:
        adapter = AsyncMock()
        # Vehicle ts 200s behind feed header on the 2nd poll -> suppressed
        adapter.fetch.side_effect = [
            _feed_with_trip_update("v1", "trip-1", "stopA", current_status=1),
            _feed_with_trip_update(
                "v1",
                "trip-1",
                "stopB",
                vehicle_ts=1704067200,
                feed_ts=1704067400,
                current_status=1,
            ),
        ]
        broadcaster = _mock_broadcaster()
        config = _settings(infer_in_transit_from_trip_updates=True)

        call_count = {"n": 0}

        async def sleep_side_effect(_: float) -> None:
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise asyncio.CancelledError()

        with patch("nibble.poller.asyncio.sleep", new=sleep_side_effect):
            with pytest.raises(asyncio.CancelledError):
                await poll_loop(config, StaticGTFS(), broadcaster, adapter=adapter)

        # With stalled suppression, no IN_TRANSIT_TO update should fire
        if len(broadcaster.broadcast.call_args_list) >= 2:
            second_call_events = broadcaster.broadcast.call_args_list[1][0][0]
            statuses = [
                ev.data.get("attributes", {}).get("current_status")
                for ev in second_call_events
                if ev.event_type == "update" and isinstance(ev.data, dict)
            ]
            assert "IN_TRANSIT_TO" not in statuses


class TestPollLoopGtfsHolder:
    async def test_gtfs_holder_attribute_is_read(self) -> None:
        """When gtfs has a .gtfs attribute (GtfsHolder), it should read gtfs.gtfs."""
        adapter = AsyncMock()
        adapter.fetch.return_value = _feed_with_vehicle()
        broadcaster = _mock_broadcaster()

        static = StaticGTFS()
        gtfs_holder = MagicMock()
        gtfs_holder.gtfs = static

        await _run_one_cycle(_settings(), gtfs_holder, broadcaster, adapter)

        broadcaster.broadcast.assert_called_once()
