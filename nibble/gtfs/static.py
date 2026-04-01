"""Downloads and parses static GTFS ZIP archives into in-memory indexes."""

from __future__ import annotations

import csv
import io
import logging
import math
import zipfile
import zoneinfo
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import httpx

from nibble.models import StopTime, Trip

logger = logging.getLogger(__name__)


@dataclass
class StaticGTFS:
    """In-memory indexes built from a static GTFS ZIP at startup.

    Attributes:
        trips: Mapping of ``trip_id`` → :class:`~nibble.models.Trip`, loaded
            from ``trips.txt``.
        stop_times: Mapping of ``trip_id`` → list of
            :class:`~nibble.models.StopTime` sorted by ``stop_sequence``,
            loaded from ``stop_times.txt``.
        stops: Mapping of ``stop_id`` → ``(lat, lon)`` float tuple, loaded
            from ``stops.txt``.
        shapes: Mapping of ``shape_id`` → list of ``(lat, lon)`` tuples sorted
            by ``shape_pt_sequence``, loaded from ``shapes.txt``.
        route_trips: Mapping of ``route_id`` → list of ``trip_id`` strings for
            all trips on that route.  Built from ``trips.txt`` at load time.
        route_short_names: Mapping of ``route_short_name`` → ``route_id`` for
            agencies whose GTFS-RT feeds report short names instead of the
            internal route UUID/ID.  Built from ``routes.txt`` at load time.
    """

    trips: dict[str, Trip] = field(default_factory=dict)
    stop_times: dict[str, list[StopTime]] = field(default_factory=dict)
    stops: dict[str, tuple[float, float]] = field(default_factory=dict)
    shapes: dict[str, list[tuple[float, float]]] = field(default_factory=dict)
    route_trips: dict[str, list[str]] = field(default_factory=dict)
    route_short_names: dict[str, str] = field(default_factory=dict)


def last_stop_sequence(gtfs: StaticGTFS, trip_id: str) -> int | None:
    """Return the stop_sequence of the final stop for *trip_id*, or ``None`` if unknown.

    Args:
        gtfs: The loaded static GTFS indexes.
        trip_id: The GTFS trip identifier to look up.

    Returns:
        The highest ``stop_sequence`` value for the trip, or ``None`` if the
        trip has no stop-time data.
    """
    times = gtfs.stop_times.get(trip_id)
    if not times:
        return None
    return times[-1].stop_sequence  # stop_times lists are sorted by stop_sequence at load time


def load_static_gtfs(
    url: str,
    auth: httpx.Auth | None = None,
    fill_shape_dist_traveled: bool = True,
) -> StaticGTFS:
    """Download and parse a static GTFS ZIP from a URL. Synchronous; runs at startup.

    Args:
        url: URL of the static GTFS ZIP archive.
        auth: Optional httpx auth strategy (e.g. query-param or header key).
        fill_shape_dist_traveled: When ``True`` (the default), back-fill
            ``shape_dist_traveled`` for stop times that lack it.  Pass
            ``False`` for feeds that already include complete values.

    Returns:
        A ``StaticGTFS`` object with populated trip and stop-time indexes.

    Raises:
        httpx.HTTPStatusError: If the download returns a non-2xx response.
    """
    logger.info("Downloading static GTFS from %s", url)
    response = httpx.get(url, auth=auth, follow_redirects=True, timeout=60)
    response.raise_for_status()
    return _parse_gtfs_zip(response.content, fill_shape_dist_traveled=fill_shape_dist_traveled)


def load_static_gtfs_from_bytes(
    content: bytes,
    fill_shape_dist_traveled: bool = True,
) -> StaticGTFS:
    """Parse a static GTFS ZIP from already-downloaded bytes.

    Use this when the ZIP was already fetched (e.g. by the fixer/publisher)
    to avoid downloading it a second time.

    Args:
        content: Raw bytes of a GTFS ZIP archive.
        fill_shape_dist_traveled: When ``True`` (the default), back-fill
            ``shape_dist_traveled`` for stop times that lack it.  Pass
            ``False`` for feeds that already include complete values.

    Returns:
        A ``StaticGTFS`` object with populated trip and stop-time indexes.
    """
    return _parse_gtfs_zip(content, fill_shape_dist_traveled=fill_shape_dist_traveled)


_STOPPED_THRESHOLD_M = 30.0


def infer_stop_from_position(
    lat: float,
    lon: float,
    trip_id: str,
    gtfs: StaticGTFS,
) -> tuple[str | None, int | None, Literal["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"]]:
    """Infer stop_id, stop_sequence, and current_status from a raw vehicle position.

    Projects the vehicle onto the trip's shape polyline using
    ``shape_dist_traveled``, then determines the next stop ahead and whether
    the vehicle is stopped at it.

    Returns ``(None, None, "IN_TRANSIT_TO")`` when inference is not possible
    (trip not found, no shape, no stop times, or stop times lack
    ``shape_dist_traveled``).

    Args:
        lat: Vehicle latitude in WGS84 decimal degrees.
        lon: Vehicle longitude in WGS84 decimal degrees.
        trip_id: GTFS trip identifier for the vehicle's current trip.
        gtfs: Loaded static GTFS indexes.

    Returns:
        A ``(stop_id, stop_sequence, current_status)`` tuple.  ``current_status``
        is ``"STOPPED_AT"`` when the vehicle is within
        :data:`_STOPPED_THRESHOLD_M` metres of a stop, otherwise
        ``"IN_TRANSIT_TO"``.
    """
    trip = gtfs.trips.get(trip_id)
    if trip is None or trip.shape_id is None:
        return None, None, "IN_TRANSIT_TO"

    shape_pts = gtfs.shapes.get(trip.shape_id)
    if not shape_pts:
        return None, None, "IN_TRANSIT_TO"

    times = gtfs.stop_times.get(trip_id)
    if not times:
        return None, None, "IN_TRANSIT_TO"

    timed = sorted(
        (st for st in times if st.shape_dist_traveled is not None),
        key=lambda st: st.shape_dist_traveled or 0.0,
    )
    if not timed:
        return None, None, "IN_TRANSIT_TO"

    vehicle_dist = _project_onto_polyline(lat, lon, shape_pts)

    for st in timed:
        if abs((st.shape_dist_traveled or 0.0) - vehicle_dist) <= _STOPPED_THRESHOLD_M:
            return st.stop_id, st.stop_sequence, "STOPPED_AT"

    for st in timed:
        if (st.shape_dist_traveled or 0.0) > vehicle_dist:
            return st.stop_id, st.stop_sequence, "IN_TRANSIT_TO"

    # Vehicle is past the last stop - report the last stop
    last = timed[-1]
    return last.stop_id, last.stop_sequence, "IN_TRANSIT_TO"


_TRIP_TIME_WINDOW_S = 1800  # 30 minutes tolerance on each end of a trip's time window


def _gtfs_time_to_seconds(time_str: str | None) -> int | None:
    """Convert a GTFS ``HH:MM:SS`` time string to seconds past midnight.

    Hours may exceed 23 for service running past midnight (e.g. ``"25:30:00"``
    → 91800).  Returns ``None`` for ``None`` or malformed input.
    """
    if not time_str:
        return None
    parts = time_str.split(":")
    if len(parts) != 3:
        return None
    try:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return None


_BEARING_TOLERANCE_DEG = 90.0


def infer_trip_from_position(
    lat: float,
    lon: float,
    route_id: str,
    gtfs: StaticGTFS,
    timestamp: datetime | None = None,
    agency_timezone: str | None = None,
    bearing: float | None = None,
) -> str | None:
    """Infer the most likely ``trip_id`` for a vehicle given its position and route.

    Iterates over all trips for *route_id* that have an associated shape and
    computes the minimum perpendicular distance from *(lat, lon)* to each
    shape polyline.

    When both *timestamp* and *agency_timezone* are supplied, trips whose
    scheduled time window (first-stop to last-stop departure/arrival times,
    with a ±30-minute tolerance) does not bracket the current local time are
    excluded before the distance ranking.  When no trips survive the time
    filter the constraint is dropped and all candidates are ranked by distance
    alone, so the function always returns the best geometric match.

    Returns ``None`` only when *route_id* is not in the index or no candidate
    trip has a shape.

    Note: if two trips share the same shape (same physical path, different
    times of day) the returned ``trip_id`` is one of them arbitrarily, but
    stop inference via :func:`infer_stop_from_position` will still be correct
    since the shapes are identical.

    Args:
        lat: Vehicle latitude in WGS84 decimal degrees.
        lon: Vehicle longitude in WGS84 decimal degrees.
        route_id: GTFS route identifier reported by the feed.
        gtfs: Loaded static GTFS indexes.
        timestamp: UTC-aware datetime of the observation.  Required for
            time-of-day filtering.
        agency_timezone: IANA timezone string (e.g. ``"America/New_York"``).
            Required for time-of-day filtering.
        bearing: Vehicle heading in degrees clockwise from north (0-359), or
            ``None`` if not reported.  When provided, trips whose shape runs
            more than :data:`_BEARING_TOLERANCE_DEG` degrees opposite to the
            vehicle's heading at the projected point are excluded.  The filter
            is skipped when no candidates survive it.

    Returns:
        The best-matching ``trip_id``, or ``None`` if no match is possible.
    """
    trip_ids = gtfs.route_trips.get(route_id)
    if not trip_ids:
        return None

    # Compute local time-of-day in seconds when we have enough context
    local_tod: int | None = None
    if timestamp is not None and agency_timezone is not None:
        try:
            tz = zoneinfo.ZoneInfo(agency_timezone)
            local_dt = timestamp.astimezone(tz)
            local_tod = local_dt.hour * 3600 + local_dt.minute * 60 + local_dt.second
        except Exception:
            logger.warning("Unknown agency_timezone %r; skipping time filter", agency_timezone)

    def _in_time_window(trip_id: str) -> bool:
        """Return True if local_tod falls within this trip's scheduled window."""
        times = gtfs.stop_times.get(trip_id)
        if not times:
            return True  # no schedule data - don't filter out
        secs = []
        for st in times:
            s = _gtfs_time_to_seconds(st.departure_time or st.arrival_time)
            if s is not None:
                secs.append(s)
        if not secs:
            return True
        if local_tod is None:
            return True
        first, last = min(secs), max(secs)
        # Check both the current calendar day and the "extended" day (handles
        # post-midnight trips that use HH > 23 in GTFS)
        for tod in (local_tod, local_tod + 86400):
            if first - _TRIP_TIME_WINDOW_S <= tod <= last + _TRIP_TIME_WINDOW_S:
                return True
        return False

    def _score(trip_id: str) -> float:
        shape_id = gtfs.trips[trip_id].shape_id
        if shape_id is None:
            return math.inf
        shape_pts = gtfs.shapes.get(shape_id)
        if not shape_pts:
            return math.inf
        return _min_distance_to_polyline(lat, lon, shape_pts)

    scores = {tid: _score(tid) for tid in trip_ids}
    candidates = [tid for tid, s in scores.items() if s < math.inf]
    if not candidates:
        return None

    # Apply time filter; fall back to all candidates if none survive
    if local_tod is not None:
        time_filtered = [tid for tid in candidates if _in_time_window(tid)]
        if time_filtered:
            candidates = time_filtered
        else:
            logger.debug(
                "infer_trip_from_position: time filter eliminated all candidates for "
                "route %r at tod=%ds; falling back to geometry-only",
                route_id,
                local_tod,
            )

    # Apply bearing filter; fall back to all remaining candidates if none survive
    if bearing is not None:
        bearing_filtered = []
        for tid in candidates:
            shape_id = gtfs.trips[tid].shape_id
            if shape_id is None:
                bearing_filtered.append(tid)
                continue
            shape_pts = gtfs.shapes.get(shape_id)
            if not shape_pts:
                bearing_filtered.append(tid)
                continue
            shape_bearing = _shape_bearing_at_projection(lat, lon, shape_pts)
            if (
                shape_bearing is None
                or _angle_difference(bearing, shape_bearing) <= _BEARING_TOLERANCE_DEG
            ):
                bearing_filtered.append(tid)
        if bearing_filtered:
            candidates = bearing_filtered
        else:
            logger.debug(
                "infer_trip_from_position: bearing filter eliminated all candidates for "
                "route %r (vehicle bearing=%.1f); falling back to time-filtered set",
                route_id,
                bearing,
            )

    return min(candidates, key=scores.__getitem__)


def _min_distance_to_polyline(
    lat: float, lon: float, shape_pts: list[tuple[float, float]]
) -> float:
    """Return the minimum perpendicular distance in metres from a point to a polyline.

    Uses the same flat-earth approximation as :func:`_project_onto_polyline`.
    """
    if len(shape_pts) == 1:
        return _haversine_m(lat, lon, shape_pts[0][0], shape_pts[0][1])

    best_dist_sq = math.inf
    R = 6_371_000.0
    RAD = math.pi / 180.0

    for i in range(len(shape_pts) - 1):
        lat1, lon1 = shape_pts[i]
        lat2, lon2 = shape_pts[i + 1]
        cos_lat = math.cos(math.radians((lat1 + lat2) / 2.0))

        ax = (lat - lat1) * R * RAD
        ay = (lon - lon1) * R * RAD * cos_lat
        bx = (lat2 - lat1) * R * RAD
        by = (lon2 - lon1) * R * RAD * cos_lat

        seg_len_sq = bx * bx + by * by
        if seg_len_sq == 0.0:
            t = 0.0
        else:
            t = max(0.0, min(1.0, (ax * bx + ay * by) / seg_len_sq))

        dx = ax - bx * t
        dy = ay - by * t
        d_sq = dx * dx + dy * dy
        if d_sq < best_dist_sq:
            best_dist_sq = d_sq

    return math.sqrt(best_dist_sq)


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in metres between two WGS84 points."""
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(min(1.0, a)))


def _shape_bearing_at_projection(
    lat: float, lon: float, shape_pts: list[tuple[float, float]]
) -> float | None:
    """Return the bearing (degrees, 0-359) of the shape at the point closest to (lat, lon).

    Finds the segment with minimum perpendicular distance using the same
    flat-earth approximation as :func:`_min_distance_to_polyline`, then
    returns the haversine initial bearing of that segment.  Returns ``None``
    for degenerate shapes with fewer than two points.
    """
    if len(shape_pts) < 2:
        return None

    best_dist_sq = math.inf
    best_seg = 0
    R = 6_371_000.0
    RAD = math.pi / 180.0

    for i in range(len(shape_pts) - 1):
        lat1, lon1 = shape_pts[i]
        lat2, lon2 = shape_pts[i + 1]
        cos_lat = math.cos(math.radians((lat1 + lat2) / 2.0))

        ax = (lat - lat1) * R * RAD
        ay = (lon - lon1) * R * RAD * cos_lat
        bx = (lat2 - lat1) * R * RAD
        by = (lon2 - lon1) * R * RAD * cos_lat

        seg_len_sq = bx * bx + by * by
        t = max(0.0, min(1.0, (ax * bx + ay * by) / seg_len_sq)) if seg_len_sq else 0.0
        dx = ax - bx * t
        dy = ay - by * t
        d_sq = dx * dx + dy * dy
        if d_sq < best_dist_sq:
            best_dist_sq = d_sq
            best_seg = i

    lat1, lon1 = shape_pts[best_seg]
    lat2, lon2 = shape_pts[best_seg + 1]
    dlon = math.radians(lon2 - lon1)
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _angle_difference(a: float, b: float) -> float:
    """Return the absolute angular difference between two bearings (0-180 degrees)."""
    diff = abs(a - b) % 360
    return diff if diff <= 180 else 360 - diff


def _project_onto_polyline(
    stop_lat: float,
    stop_lon: float,
    shape_pts: list[tuple[float, float]],
    cum: list[float] | None = None,
) -> float:
    """Return the distance in metres along *shape_pts* to the closest point.

    Uses a flat-earth (Cartesian) approximation per segment, which is accurate
    to well within one metre for the segment lengths typical in GTFS shapes.

    Args:
        stop_lat: Stop latitude in WGS84 decimal degrees.
        stop_lon: Stop longitude in WGS84 decimal degrees.
        shape_pts: Ordered list of ``(lat, lon)`` shape points.
        cum: Optional precomputed cumulative vertex distances (metres). When
            projecting many stops onto the same shape, pass this to avoid
            recomputing it for each stop.

    Returns:
        Cumulative distance in metres along the polyline to the projection of
        the stop onto the nearest segment.
    """
    if len(shape_pts) == 1:
        return 0.0

    if cum is None:
        cum = [0.0]
        for i in range(1, len(shape_pts)):
            cum.append(cum[-1] + _haversine_m(*shape_pts[i - 1], *shape_pts[i]))

    best_dist_sq = math.inf
    best_along = 0.0
    R = 6_371_000.0
    RAD = math.pi / 180.0

    for i in range(len(shape_pts) - 1):
        lat1, lon1 = shape_pts[i]
        lat2, lon2 = shape_pts[i + 1]
        cos_lat = math.cos(math.radians((lat1 + lat2) / 2.0))

        ax = (stop_lat - lat1) * R * RAD
        ay = (stop_lon - lon1) * R * RAD * cos_lat
        bx = (lat2 - lat1) * R * RAD
        by = (lon2 - lon1) * R * RAD * cos_lat

        seg_len_sq = bx * bx + by * by
        if seg_len_sq == 0.0:
            t = 0.0
            seg_along = 0.0
        else:
            t = max(0.0, min(1.0, (ax * bx + ay * by) / seg_len_sq))
            seg_along = t * math.sqrt(seg_len_sq)

        dx = ax - bx * t
        dy = ay - by * t
        d_sq = dx * dx + dy * dy
        if d_sq < best_dist_sq:
            best_dist_sq = d_sq
            best_along = cum[i] + seg_along

    return best_along


def _fill_shape_dist_traveled(gtfs: StaticGTFS) -> None:
    """Back-fill ``shape_dist_traveled`` for stop times that lack it.

    For each trip that has a ``shape_id`` and at least one stop time with a
    ``None`` ``shape_dist_traveled``, project every stop in that trip onto the
    shape polyline and assign the cumulative distance (in metres) to all stop
    times for that trip.  Trips whose every stop time already has a value, or
    whose shape or stop coordinates are unavailable, are left unchanged.
    """
    needs_fill = [
        trip_id
        for trip_id, times in gtfs.stop_times.items()
        if not all(st.shape_dist_traveled is not None for st in times)
    ]
    if not needs_fill:
        return
    logger.info("Back-filling shape_dist_traveled for %d trips", len(needs_fill))

    # Cache cumulative vertex distances per shape_id so trips sharing a shape
    # don't recompute O(S) haversine calls each.
    cum_cache: dict[str, list[float]] = {}
    # Cache stop projections per (stop_id, shape_id) — many trips share both
    # the same shape and the same stops (e.g. all ACE trips hit the same stations).
    projection_cache: dict[tuple[str, str], float | None] = {}

    for trip_id in needs_fill:
        times = gtfs.stop_times[trip_id]
        if all(st.shape_dist_traveled is not None for st in times):
            continue

        trip = gtfs.trips.get(trip_id)
        if trip is None or trip.shape_id is None:
            continue

        shape_id = trip.shape_id
        shape_pts = gtfs.shapes.get(shape_id)
        if not shape_pts:
            continue

        if shape_id not in cum_cache:
            cum: list[float] = [0.0]
            for i in range(1, len(shape_pts)):
                cum.append(cum[-1] + _haversine_m(*shape_pts[i - 1], *shape_pts[i]))
            cum_cache[shape_id] = cum
        cum = cum_cache[shape_id]

        # Partial feed coverage: discard existing values and recompute the whole trip
        for st in times:
            cache_key = (st.stop_id, shape_id)
            if cache_key not in projection_cache:
                coords = gtfs.stops.get(st.stop_id)
                if coords is None:
                    logger.warning(
                        "Stop %r on trip %r has no coordinates in stops.txt; "
                        "shape_dist_traveled will be None and stop inference may be impaired",
                        st.stop_id,
                        trip_id,
                    )
                    projection_cache[cache_key] = None
                else:
                    projection_cache[cache_key] = _project_onto_polyline(
                        coords[0], coords[1], shape_pts, cum
                    )
            dist = projection_cache[cache_key]
            if dist is not None:
                st.shape_dist_traveled = dist


def _parse_gtfs_zip(content: bytes, fill_shape_dist_traveled: bool = True) -> StaticGTFS:
    """Parse a raw GTFS ZIP archive and return populated StaticGTFS indexes.

    Args:
        content: Raw bytes of a GTFS ZIP archive.
        fill_shape_dist_traveled: When ``True`` (the default), back-fill
            ``shape_dist_traveled`` for stop times that lack it.  Pass
            ``False`` for feeds that already include complete values to skip
            the projection step and reduce startup memory usage.

    Returns:
        A ``StaticGTFS`` with trips, stop_times, stops, and shapes populated
        from the corresponding ``.txt`` files. Missing files are silently
        skipped. If ``shape_dist_traveled`` is absent from ``stop_times.txt``
        and ``fill_shape_dist_traveled`` is ``True``, it is computed by
        projecting each stop onto its trip's shape polyline (in metres).
    """
    gtfs = StaticGTFS()
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = zf.namelist()

        if "stops.txt" in names:
            logger.info("Parsing stops.txt")
            with zf.open("stops.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    stop_id = row.get("stop_id", "").strip()
                    if not stop_id:
                        continue
                    try:
                        lat = float(row.get("stop_lat", "").strip())
                        lon = float(row.get("stop_lon", "").strip())
                    except ValueError:
                        continue
                    gtfs.stops[stop_id] = (lat, lon)

        if "routes.txt" in names:
            logger.info("Parsing routes.txt")
            with zf.open("routes.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    route_id = row.get("route_id", "").strip()
                    if not route_id:
                        continue
                    # Index by both short and long name so feeds that report either
                    # value as their route identifier can be resolved.
                    for col in ("route_short_name", "route_long_name"):
                        name = row.get(col, "").strip()
                        if name:
                            gtfs.route_short_names[name] = route_id

        if "shapes.txt" in names:
            logger.info("Parsing shapes.txt")
            raw_shapes: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
            with zf.open("shapes.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    shape_id = row.get("shape_id", "").strip()
                    seq_raw = row.get("shape_pt_sequence", "").strip()
                    if not shape_id or not seq_raw.isdigit():
                        continue
                    try:
                        lat = float(row.get("shape_pt_lat", "").strip())
                        lon = float(row.get("shape_pt_lon", "").strip())
                    except ValueError:
                        continue
                    raw_shapes[shape_id].append((int(seq_raw), lat, lon))
            for shape_id, pts in raw_shapes.items():
                pts.sort(key=lambda p: p[0])
                gtfs.shapes[shape_id] = [(lat, lon) for _, lat, lon in pts]

        if "trips.txt" in names:
            logger.info("Parsing trips.txt")
            with zf.open("trips.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    trip_id = row.get("trip_id", "").strip()
                    route_id = row.get("route_id", "").strip()
                    if not trip_id or not route_id:
                        continue
                    direction_raw = row.get("direction_id", "").strip()
                    direction_id = int(direction_raw) if direction_raw.isdigit() else None
                    gtfs.trips[trip_id] = Trip(
                        trip_id=trip_id,
                        route_id=route_id,
                        direction_id=direction_id,
                        shape_id=row.get("shape_id", "").strip() or None,
                    )
            # Build reverse index: route_id → [trip_id, ...]
            rt: dict[str, list[str]] = defaultdict(list)
            for tid, trip in gtfs.trips.items():
                rt[trip.route_id].append(tid)
            gtfs.route_trips = dict(rt)

        if "stop_times.txt" in names:
            logger.info("Parsing stop_times.txt")
            raw: dict[str, list[StopTime]] = defaultdict(list)
            with zf.open("stop_times.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    trip_id = row.get("trip_id", "").strip()
                    stop_id = row.get("stop_id", "").strip()
                    seq_raw = row.get("stop_sequence", "").strip()
                    if not trip_id or not stop_id or not seq_raw.isdigit():
                        continue
                    dist_raw = row.get("shape_dist_traveled", "").strip()
                    try:
                        shape_dist_traveled = float(dist_raw) if dist_raw else None
                    except ValueError:
                        shape_dist_traveled = None
                    raw[trip_id].append(
                        StopTime(
                            trip_id=trip_id,
                            stop_id=stop_id,
                            stop_sequence=int(seq_raw),
                            arrival_time=row.get("arrival_time", "").strip() or None,
                            departure_time=row.get("departure_time", "").strip() or None,
                            shape_dist_traveled=shape_dist_traveled,
                        )
                    )
            for trip_id, times in raw.items():
                gtfs.stop_times[trip_id] = sorted(times, key=lambda st: st.stop_sequence)

    if fill_shape_dist_traveled:
        logger.info("Computing shape_dist_traveled for trips missing it")
        _fill_shape_dist_traveled(gtfs)

    logger.info(
        "Loaded static GTFS: %d trips, %d trips with stop times, %d stops, %d shapes, "
        "%d route short names (%s)",
        len(gtfs.trips),
        len(gtfs.stop_times),
        len(gtfs.stops),
        len(gtfs.shapes),
        len(gtfs.route_short_names),
        ", ".join(sorted(gtfs.route_short_names)[:10]) or "none",
    )
    return gtfs
