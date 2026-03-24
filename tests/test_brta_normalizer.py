"""Tests for nibble.normalizer.brta."""

from __future__ import annotations

import pytest
from nibble.protos import gtfs_realtime_pb2

from nibble.normalizer.brta import BrtaNormalizer, _candidate_short_name

# --- _candidate_short_name unit tests ---


@pytest.mark.parametrize(
    "route_id, expected",
    [
        ("Wk Rt 01", "1"),
        ("Wk Rt 02", "2"),
        ("Wk Rt 12", "12"),
        ("Wk Rt 21", "21"),
        ("Rte 34", "34"),
        ("Route 5 Loop", "5"),
        ("Rte 21 Express", "21"),
        ("Rte 5A", "5A"),
    ],
)
def test_candidate_short_name(route_id: str, expected: str) -> None:
    assert _candidate_short_name(route_id) == expected


def test_candidate_short_name_no_number() -> None:
    assert _candidate_short_name("Unknown Route") is None


# --- Helpers ---


def _make_feed(route_id: str, trip_id: str = "") -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "1"
    entity.vehicle.trip.route_id = route_id
    if trip_id:
        entity.vehicle.trip.trip_id = trip_id
    return feed


class _FakeGTFS:
    def __init__(self, route_trips: dict, route_short_names: dict) -> None:
        self.route_trips = route_trips
        self.route_short_names = route_short_names


# --- Normalizer tests ---


def test_remaps_wk_rt_prefix() -> None:
    feed = _make_feed("Wk Rt 01")
    gtfs = _FakeGTFS(route_trips={}, route_short_names={"1": "1"})
    result = BrtaNormalizer().normalize(feed, gtfs)
    assert result.entity[0].vehicle.trip.route_id == "1"


def test_remaps_rte_prefix() -> None:
    feed = _make_feed("Rte 34")
    gtfs = _FakeGTFS(route_trips={}, route_short_names={"34": "34"})
    result = BrtaNormalizer().normalize(feed, gtfs)
    assert result.entity[0].vehicle.trip.route_id == "34"


def test_already_valid_route_id_skipped() -> None:
    feed = _make_feed("1")
    gtfs = _FakeGTFS(route_trips={"1": ["t1"]}, route_short_names={"1": "1"})
    result = BrtaNormalizer().normalize(feed, gtfs)
    assert result.entity[0].vehicle.trip.route_id == "1"


def test_trip_id_always_cleared() -> None:
    feed = _make_feed("Wk Rt 01", trip_id="Rte 01 1130 in")
    gtfs = _FakeGTFS(route_trips={}, route_short_names={"1": "1"})
    result = BrtaNormalizer().normalize(feed, gtfs)
    assert result.entity[0].vehicle.trip.trip_id == ""


def test_trip_id_cleared_even_when_route_already_valid() -> None:
    feed = _make_feed("1", trip_id="Rte 01 1130 in")
    gtfs = _FakeGTFS(route_trips={"1": ["t1"]}, route_short_names={"1": "1"})
    result = BrtaNormalizer().normalize(feed, gtfs)
    assert result.entity[0].vehicle.trip.trip_id == ""


def test_unmatched_route_id_logged_but_unchanged() -> None:
    feed = _make_feed("Route 5 Loop")
    gtfs = _FakeGTFS(route_trips={}, route_short_names={"5A": "5A", "5B": "5B"})
    result = BrtaNormalizer().normalize(feed, gtfs)
    # "5" not in short_names; route_id left as-is
    assert result.entity[0].vehicle.trip.route_id == "Route 5 Loop"


def test_unextractable_route_id_unchanged() -> None:
    feed = _make_feed("Unknown Route")
    gtfs = _FakeGTFS(route_trips={}, route_short_names={})
    result = BrtaNormalizer().normalize(feed, gtfs)
    assert result.entity[0].vehicle.trip.route_id == "Unknown Route"
