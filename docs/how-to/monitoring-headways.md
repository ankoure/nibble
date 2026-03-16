# Monitoring headways

A **headway** is the time or distance gap between consecutive vehicles on a route. Monitoring headways lets you detect two common service quality problems:

- **Bunching** — two vehicles running very close together, leaving a large gap behind them
- **Gapping** — an unusually large hole in service, often because a vehicle has dropped off the route

Nibble computes headways from the current resolved vehicle snapshot using static GTFS shape distances and scheduled departure times.

---

## How it works

`compute_headways` filters the snapshot to vehicles on the requested route that have a resolved `trip_id`. It groups them by `direction_id`, then sorts each group **furthest-ahead first** using `shape_dist_traveled` at the vehicle's current stop. If `shape_dist_traveled` is not present in the agency's GTFS, it falls back to stop sequence.

Consecutive pairs in each sorted group are annotated with two gap metrics:

- `gap_to_previous_meters` — physical distance along the route shape between the two vehicles' current stops
- `scheduled_gap_to_previous_seconds` — difference in scheduled departure times at each vehicle's current stop

The lead vehicle in each direction (furthest ahead) has `null` for both gap fields.

---

## `compute_headways`

```python
from nibble.headways import compute_headways

result = compute_headways(route_id="39", snapshot=snapshot, gtfs=gtfs)
```

**Arguments:**

| Argument | Type | Description |
|---|---|---|
| `route_id` | string | The GTFS route to compute headways for |
| `snapshot` | dict | Current vehicle snapshot keyed by `vehicle_id` |
| `gtfs` | StaticGTFS | Loaded static GTFS indexes |

**Return shape:**

```json
{
  "route_id": "39",
  "directions": [
    {
      "direction_id": 0,
      "vehicles": [
        {
          "vehicle_id": "y1234",
          "trip_id": "trip-A",
          "stop_id": "stop-10",
          "stop_sequence": 18,
          "shape_dist_traveled": 4821.3,
          "scheduled_departure": "14:32:00",
          "gap_to_previous_meters": null,
          "scheduled_gap_to_previous_seconds": null
        },
        {
          "vehicle_id": "y5678",
          "trip_id": "trip-B",
          "stop_id": "stop-7",
          "stop_sequence": 12,
          "shape_dist_traveled": 2904.1,
          "scheduled_departure": "14:20:00",
          "gap_to_previous_meters": 1917.2,
          "scheduled_gap_to_previous_seconds": 720
        }
      ]
    }
  ]
}
```

**Per-vehicle fields:**

| Field | Type | Description |
|---|---|---|
| `vehicle_id` | string | Vehicle identifier |
| `trip_id` | string | Resolved trip identifier |
| `stop_id` | string or null | Current stop |
| `stop_sequence` | int or null | Current stop sequence |
| `shape_dist_traveled` | float or null | Distance along the shape to the current stop (from GTFS) |
| `scheduled_departure` | string or null | Scheduled departure time at the current stop (HH:MM:SS) |
| `gap_to_previous_meters` | float or null | Physical distance to the vehicle ahead; `null` for the lead vehicle or if `shape_dist_traveled` is missing |
| `scheduled_gap_to_previous_seconds` | int or null | Scheduled time gap to the vehicle ahead; `null` for the lead vehicle or if schedule data is missing |

---

## Interpreting the results

**`gap_to_previous_meters`** measures where vehicles physically are on the route. A small value means two vehicles are close together.

**`scheduled_gap_to_previous_seconds`** measures how far apart the vehicles *should* be according to the schedule, based on when their current stops are timed. This is the more operationally useful metric:

- **Positive** — the vehicle ahead is scheduled later than the one behind (normal, expected spacing)
- **Near zero or negative** — the two vehicles are scheduled at almost the same time; this is bunching relative to the schedule regardless of physical distance

A common pattern is to flag any pair where `scheduled_gap_to_previous_seconds` is below a threshold (e.g. 120 seconds) as bunched.

**Missing values:** `gap_to_previous_meters` is `null` when `shape_dist_traveled` is absent from the agency's GTFS. `scheduled_gap_to_previous_seconds` is `null` when scheduled times are missing. Check your GTFS export if you see consistent nulls.

---

## Example

```python
from nibble.headways import compute_headways

BUNCHING_THRESHOLD_SECONDS = 120  # flag pairs closer than 2 minutes scheduled

result = compute_headways(route_id="39", snapshot=snapshot, gtfs=gtfs)

for direction in result["directions"]:
    print(f"Direction {direction['direction_id']}")
    for vehicle in direction["vehicles"]:
        gap = vehicle["scheduled_gap_to_previous_seconds"]
        if gap is None:
            continue
        status = "BUNCHED" if gap < BUNCHING_THRESHOLD_SECONDS else "ok"
        print(
            f"  {vehicle['vehicle_id']} (trip {vehicle['trip_id']}): "
            f"scheduled gap {gap}s [{status}]"
        )
```

**Example output:**

```
Direction 0
  y1234 (trip trip-A): lead vehicle
  y5678 (trip trip-B): scheduled gap 720s [ok]
  y9012 (trip trip-C): scheduled gap 85s [BUNCHED]
```
