from __future__ import annotations

from nibble import unknown_routes


class TestUnknownRoutes:
    def setup_method(self) -> None:
        unknown_routes.clear()

    def test_record_new_route_count_is_one(self) -> None:
        unknown_routes.record("route-X")
        entries = unknown_routes.all_entries()
        assert len(entries) == 1
        assert entries[0]["route_id"] == "route-X"
        assert entries[0]["count"] == 1

    def test_record_same_route_twice_increments_count(self) -> None:
        unknown_routes.record("route-X")
        unknown_routes.record("route-X")
        entries = unknown_routes.all_entries()
        assert entries[0]["count"] == 2

    def test_last_seen_updates_on_second_record(self) -> None:
        unknown_routes.record("route-X")
        first_seen = unknown_routes.all_entries()[0]["first_seen"]
        unknown_routes.record("route-X")
        entry = unknown_routes.all_entries()[0]
        # first_seen stays the same; last_seen may be equal or later
        assert entry["first_seen"] == first_seen
        assert entry["last_seen"] >= first_seen

    def test_all_entries_sorted_by_descending_count(self) -> None:
        unknown_routes.record("route-A")
        unknown_routes.record("route-B")
        unknown_routes.record("route-B")
        unknown_routes.record("route-C")
        unknown_routes.record("route-C")
        unknown_routes.record("route-C")
        entries = unknown_routes.all_entries()
        counts = [e["count"] for e in entries]
        assert counts == sorted(counts, reverse=True)

    def test_all_entries_has_required_keys(self) -> None:
        unknown_routes.record("route-X")
        entry = unknown_routes.all_entries()[0]
        assert set(entry.keys()) == {"route_id", "count", "first_seen", "last_seen"}

    def test_clear_empties_registry(self) -> None:
        unknown_routes.record("route-X")
        unknown_routes.clear()
        assert unknown_routes.all_entries() == []

    def test_all_entries_empty_when_nothing_recorded(self) -> None:
        assert unknown_routes.all_entries() == []
