# Manual trip assignment corrections

Sometimes nibble cannot automatically determine which trip a vehicle is running. This happens when:

- The GTFS-RT feed temporarily omits `trip_id` (common at terminals or during AVL glitches)
- Position inference has multiple plausible candidates and picks the wrong one
- A vehicle is reassigned mid-route and the feed hasn't caught up yet

In these cases, an operator can issue a **manual trip assignment correction** via the API. Nibble applies it immediately on the next poll — overriding automatic inference — and auto-expires it once the vehicle reaches the final stop of the assigned trip.

---

## How it works

Manual corrections slot into the **resolution ladder** as step 0, evaluated before any feed data or position inference:

```
0. Manual override present   → confidence="confirmed", provenance="manual"
1. trip_id in feed + in GTFS → confidence="confirmed", provenance="observed"
2. trip_id in feed, not GTFS → confidence="confirmed", provenance="observed" (warning logged)
3. No trip_id, within stale threshold → confidence="inferred", provenance="inferred"
4. No trip_id, beyond threshold       → confidence="stale"
```

When an override is active, the resulting vehicle event carries:

- `provenance: "manual"` — so downstream consumers know the assignment was operator-issued
- `confidence: "confirmed"` — treated as authoritative by gobble and any other consumers

**Auto-expiry:** On every poll, nibble checks the vehicle's current `stop_sequence` against the last stop of the assigned trip in static GTFS. Once the vehicle reaches or passes that stop, the override is silently removed and the resolution ladder resumes normal operation.

---

## Typical workflow

This is designed to work with a companion web app that subscribes to nibble's `/vehicles` SSE stream.

1. The web app watches for vehicles where `confidence` is `"inferred"` or `"stale"` — these are candidates for manual correction.
2. An operator selects the correct trip from a list and submits it.
3. The web app `POST`s to nibble's `/trip_assignments` endpoint.
4. On the next poll (within `NIBBLE_POLL_INTERVAL_SECONDS`), nibble applies the correction and broadcasts an `update` event with `provenance="manual"`.
5. When the vehicle completes the trip, nibble auto-expires the correction with no operator action required.

---

## API

### Create or replace a trip assignment

```
POST /trip_assignments
Content-Type: application/json
```

**Request body:**

```json
{
  "vehicle_id": "BUS-42",
  "trip_id": "trip-123"
}
```

| Field | Type | Description |
|---|---|---|
| `vehicle_id` | string | The vehicle to correct. Must match the `id` field in SSE events. |
| `trip_id` | string | The GTFS trip to assign. Must exist in the loaded static GTFS. |

**Response `200 OK`:**

```json
{
  "vehicle_id": "BUS-42",
  "trip_id": "trip-123",
  "assigned_at": "2024-06-01T12:34:56.789012+00:00"
}
```

**Response `422 Unprocessable Entity`** — if either field is missing, or if `trip_id` is not found in the current static GTFS:

```json
{
  "error": "trip_id 'trip-999' not found in static GTFS"
}
```

Posting a correction for a `vehicle_id` that already has one **replaces** the existing assignment.

---

### List active corrections

```
GET /trip_assignments
```

Returns all currently active manual overrides.

**Response `200 OK`:**

```json
{
  "BUS-42": {
    "trip_id": "trip-123",
    "assigned_at": "2024-06-01T12:34:56.789012+00:00"
  },
  "BUS-99": {
    "trip_id": "trip-456",
    "assigned_at": "2024-06-01T11:00:00.000000+00:00"
  }
}
```

An empty object `{}` is returned when no corrections are active.

---

### Remove a correction

```
DELETE /trip_assignments/{vehicle_id}
```

Removes the manual override for the given vehicle. The resolution ladder resumes normal operation on the next poll. No-op if no override exists.

**Response:** `204 No Content`

---

## Persistence

Corrections survive nibble restarts. They are stored as a JSON file whose path defaults to `overrides.json` in the working directory and is configurable via the `NIBBLE_OVERRIDES_PATH` environment variable:

```bash
NIBBLE_OVERRIDES_PATH=/var/lib/nibble/overrides.json
```

Writes are atomic (write to a temp file + `os.replace`) so a crash mid-write cannot corrupt the store.

---

## Example: integrating from Python

```python
import httpx

NIBBLE_BASE = "http://localhost:8080"

# Assign a trip
resp = httpx.post(
    f"{NIBBLE_BASE}/trip_assignments",
    json={"vehicle_id": "BUS-42", "trip_id": "trip-123"},
)
resp.raise_for_status()
print(resp.json())
# {'vehicle_id': 'BUS-42', 'trip_id': 'trip-123', 'assigned_at': '...'}

# Check active corrections
resp = httpx.get(f"{NIBBLE_BASE}/trip_assignments")
print(resp.json())

# Remove early if needed
httpx.delete(f"{NIBBLE_BASE}/trip_assignments/BUS-42")
```

---

## Filtering by provenance in consumers

Downstream consumers (e.g. gobble) can filter or annotate events by `provenance` to distinguish manual corrections from feed data:

```javascript
source.addEventListener("update", (e) => {
  const vehicle = JSON.parse(e.data);
  const { provenance, confidence } = vehicle.attributes;

  if (provenance === "manual") {
    // Trip was operator-assigned — show a badge or note in the UI
  } else if (confidence === "inferred") {
    // Trip is nibble's best guess — may want to flag for review
  }
});
```
