"""Persistent store for operator-issued manual trip assignment overrides."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class OverrideStore:
    """Persists manual vehicle→trip assignment corrections to a JSON file.

    Corrections are applied by the state machine (resolution ladder step 0)
    on every poll. An entry stays active until the vehicle is observed at or
    past the last stop of the assigned trip, or until it is explicitly removed
    via :meth:`remove`.

    The backing file is written atomically (write to a temp file, then
    ``os.replace``) so a crash mid-write cannot corrupt the store.

    Attributes:
        path: Path to the JSON persistence file.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, dict[str, str]] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set(self, vehicle_id: str, trip_id: str) -> str:
        """Record a manual trip assignment for *vehicle_id*.

        Args:
            vehicle_id: The vehicle to override.
            trip_id: The trip to assign to the vehicle.

        Returns:
            ISO-8601 timestamp of when the assignment was recorded.
        """
        assigned_at = datetime.now(timezone.utc).isoformat()
        self._data[vehicle_id] = {"trip_id": trip_id, "assigned_at": assigned_at}
        self._persist()
        return assigned_at

    def get(self, vehicle_id: str) -> str | None:
        """Return the overridden trip_id for *vehicle_id*, or ``None`` if absent.

        Args:
            vehicle_id: The vehicle identifier to look up.

        Returns:
            The assigned ``trip_id`` string, or ``None`` if no active override exists.
        """
        entry = self._data.get(vehicle_id)
        return entry["trip_id"] if entry else None

    def remove(self, vehicle_id: str) -> None:
        """Remove the manual override for *vehicle_id* (no-op if absent).

        Args:
            vehicle_id: The vehicle whose override should be removed.
        """
        if vehicle_id in self._data:
            del self._data[vehicle_id]
            self._persist()

    def all(self) -> dict[str, dict[str, str]]:
        """Return a snapshot of all active overrides.

        Returns:
            A shallow copy of the internal data dict mapping vehicle IDs to
            ``{"trip_id": ..., "assigned_at": ...}`` dicts.
        """
        return dict(self._data)

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Read overrides from the backing JSON file on startup."""
        if not self.path.exists():
            return
        try:
            text = self.path.read_text(encoding="utf-8")
            loaded = json.loads(text)
            if isinstance(loaded, dict):
                self._data = loaded
                logger.info("Loaded %d override(s) from %s", len(self._data), self.path)
        except Exception:
            logger.exception(
                "Failed to load overrides from %s; starting with empty store", self.path
            )

    def _persist(self) -> None:
        """Atomically write the current overrides to the backing JSON file."""
        tmp = self.path.with_suffix(".json.tmp")
        try:
            tmp.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
            os.replace(tmp, self.path)
        except Exception:
            logger.exception("Failed to persist overrides to %s", self.path)
            tmp.unlink(missing_ok=True)
