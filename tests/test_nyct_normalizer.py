from __future__ import annotations

from nibble.gtfs.static import StaticGTFS
from nibble.models import Trip
from nibble.normalizer.nyct import NyctNormalizer
from nibble.protos import gtfs_realtime_pb2


def _gtfs(*trip_ids: str) -> StaticGTFS:
    gtfs = StaticGTFS()
    for tid in trip_ids:
        gtfs.trips[tid] = Trip(trip_id=tid, route_id=f"route-{tid}")
    return gtfs


def _feed(trip_id: str) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "v1"
    entity.vehicle.trip.trip_id = trip_id
    return feed


def _rt_trip_id(feed: gtfs_realtime_pb2.FeedMessage) -> str:
    return str(feed.entity[0].vehicle.trip.trip_id)


class TestNyctNormalizer:
    def setup_method(self) -> None:
        self.normalizer = NyctNormalizer()

    def test_subway_short_trip_id_rewritten_to_full_form(self) -> None:
        """Standard subway case: RT publishes ``067600_A..S58R``, static has the full form."""
        gtfs = _gtfs("BFA25GEN-A087-Weekday-00_067600_A..S58R")
        feed = _feed("067600_A..S58R")
        result = self.normalizer.normalize(feed, gtfs)
        assert _rt_trip_id(result) == "BFA25GEN-A087-Weekday-00_067600_A..S58R"

    def test_sir_single_dot_matches_static_double_dot(self) -> None:
        """SIR regression: RT publishes ``SI.S03R`` (single dot) while static stores
        ``SI..S03R`` (double dot) for the same trip. Without dot-run canonicalization
        the suffix index misses every SIR trip and the pod never produces data."""
        gtfs = _gtfs("SIR-FA2017-SI017-Saturday-00_081100_SI..N03R")
        feed = _feed("081100_SI.N03R")
        result = self.normalizer.normalize(feed, gtfs)
        assert _rt_trip_id(result) == "SIR-FA2017-SI017-Saturday-00_081100_SI..N03R"

    def test_trip_already_in_gtfs_unchanged(self) -> None:
        full = "BFA25GEN-A087-Weekday-00_067600_A..S58R"
        gtfs = _gtfs(full)
        feed = _feed(full)
        result = self.normalizer.normalize(feed, gtfs)
        assert _rt_trip_id(result) == full

    def test_unknown_short_id_left_unchanged(self) -> None:
        gtfs = _gtfs("BFA25GEN-A087-Weekday-00_067600_A..S58R")
        feed = _feed("999999_Z..Z99R")
        result = self.normalizer.normalize(feed, gtfs)
        assert _rt_trip_id(result) == "999999_Z..Z99R"
