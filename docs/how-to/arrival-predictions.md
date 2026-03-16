# Arrival predictions

Nibble can compute a vehicle's current delay and predict its arrival times at remaining stops using **current-delay propagation**: the delay observed at the vehicle's current stop is applied uniformly to every remaining stop in the trip.

This is a practical estimate suitable for passenger-facing displays and operational dashboards. It is not a schedule-adherence score — use it where you need "when will this bus arrive?" not "how reliably does this route run?"

---

## How it works

1. Nibble looks up the vehicle's scheduled departure time at its current stop in the static GTFS.
2. It compares that to the vehicle's observed timestamp to compute a signed delay (positive = late, negative = early).
3. That delay is added to the scheduled arrival time at every remaining stop in the trip.

The approach is intentionally simple. It assumes the vehicle continues running at the same offset from schedule — no traffic prediction, no dwell-time modelling.

---

## `compute_delay`

Returns the vehicle's current delay in integer seconds, or `None` if it cannot be determined.

```python
from nibble.predictions import compute_delay

delay = compute_delay(event, gtfs, agency_timezone="America/New_York")
# e.g. 120  → 2 minutes late
# e.g. -30  → 30 seconds early
# e.g. None → trip_id or stop_sequence missing, or stop not in schedule
```

**Returns `None` when:**

- `event.trip_id` or `event.current_stop_sequence` is `None`
- The trip has no stop-time data in the loaded static GTFS
- The current stop sequence is not found in the trip's stop times
- The scheduled time for that stop is missing

---

## `predict_arrivals`

Returns a list of predicted arrivals at all remaining stops (including the current stop), or `[]` if predictions cannot be computed.

```python
from nibble.predictions import predict_arrivals

arrivals = predict_arrivals(event, gtfs, agency_timezone="America/New_York")
```

Each entry in the list is a dict:

| Field | Type | Description |
|---|---|---|
| `stop_id` | string | GTFS stop identifier |
| `stop_sequence` | int | Stop sequence number in the trip |
| `scheduled_arrival` | string | ISO-8601 scheduled arrival time |
| `predicted_arrival` | string | ISO-8601 predicted arrival time |
| `delay_seconds` | int | Signed delay applied (positive = late) |

**Returns `[]` when:**

- `event.trip_id` or `event.current_stop_sequence` is `None`
- The trip has no stop-time data in the loaded static GTFS
- `compute_delay` returns `None`

---

## Timezone handling

Both functions accept an optional `agency_timezone` argument — an IANA timezone name such as `"America/New_York"` or `"America/Los_Angeles"`. This is used to convert the vehicle's UTC timestamp to local time-of-day for comparison against the GTFS schedule, which is expressed in local time.

If `agency_timezone` is `None` or unrecognised, both functions fall back to UTC. For agencies in non-UTC timezones this will produce incorrect delay values, so always pass the correct timezone in production.

The `NIBBLE_AGENCY_TIMEZONE` environment variable controls this for the built-in pipeline.

---

## Caveats

**Predictions inherit data quality from the vehicle state.** If a vehicle's position was interpolated or inferred, its timestamp may be stale and the computed delay will reflect that staleness. Filter by `provenance` before surfacing predictions to end users:

- `"observed"` — delay is based on a fresh feed timestamp; most reliable
- `"inferred"` — trip carried forward from a prior poll; delay may be minutes out of date
- `"interpolated"` — synthetic stop event; timestamp is estimated, not observed
- `"manual"` — operator-assigned trip; timestamp is still real, delay is usable

**Delay is not propagated across trip boundaries.** If a vehicle is on the last stop of a trip, `predict_arrivals` returns only that stop.

---

## Example

```python
import asyncio
import httpx
from nibble.predictions import compute_delay, predict_arrivals

# Assume `gtfs` is a loaded StaticGTFS and `snapshot` is the current vehicle state.
# In practice you'd obtain these from the nibble state store or a poller callback.

AGENCY_TZ = "America/New_York"

for vehicle_id, event in snapshot.items():
    if event.provenance not in ("observed", "manual"):
        continue  # skip stale or synthetic data

    delay = compute_delay(event, gtfs, AGENCY_TZ)
    if delay is None:
        continue

    print(f"{vehicle_id}: {delay:+d}s")

    arrivals = predict_arrivals(event, gtfs, AGENCY_TZ)
    for stop in arrivals[:3]:  # next 3 stops
        print(
            f"  stop {stop['stop_id']} (seq {stop['stop_sequence']}): "
            f"predicted {stop['predicted_arrival']}  "
            f"(sched {stop['scheduled_arrival']})"
        )
```
