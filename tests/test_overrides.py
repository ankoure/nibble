from __future__ import annotations

from pathlib import Path

from nibble.overrides import OverrideStore


class TestOverrideStorePersistence:
    def test_load_missing_file_starts_empty(self, tmp_path: Path) -> None:
        store = OverrideStore(tmp_path / "overrides.json")
        assert store.all() == {}

    def test_load_corrupt_json_starts_empty(self, tmp_path: Path) -> None:
        path = tmp_path / "overrides.json"
        path.write_text("not valid json", encoding="utf-8")
        store = OverrideStore(path)
        assert store.all() == {}

    def test_load_non_dict_json_starts_empty(self, tmp_path: Path) -> None:
        path = tmp_path / "overrides.json"
        path.write_text("[1, 2, 3]", encoding="utf-8")
        store = OverrideStore(path)
        assert store.all() == {}

    def test_persist_no_tmp_file_left_behind(self, tmp_path: Path) -> None:
        path = tmp_path / "overrides.json"
        store = OverrideStore(path)
        store.set("v1", "trip-1")
        tmp = path.with_suffix(".json.tmp")
        assert not tmp.exists()

    def test_round_trip_survives_reload(self, tmp_path: Path) -> None:
        path = tmp_path / "overrides.json"
        store = OverrideStore(path)
        store.set("v1", "trip-1")
        store.set("v2", "trip-2")

        reloaded = OverrideStore(path)
        assert reloaded.get("v1") == "trip-1"
        assert reloaded.get("v2") == "trip-2"

    def test_remove_persists_across_reload(self, tmp_path: Path) -> None:
        path = tmp_path / "overrides.json"
        store = OverrideStore(path)
        store.set("v1", "trip-1")
        store.remove("v1")

        reloaded = OverrideStore(path)
        assert reloaded.get("v1") is None

    def test_get_missing_vehicle_returns_none(self, tmp_path: Path) -> None:
        store = OverrideStore(tmp_path / "overrides.json")
        assert store.get("nonexistent") is None

    def test_remove_nonexistent_is_noop(self, tmp_path: Path) -> None:
        store = OverrideStore(tmp_path / "overrides.json")
        store.remove("nonexistent")  # should not raise
        assert store.all() == {}
