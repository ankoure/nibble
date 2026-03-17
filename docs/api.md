# API Reference

nibble exposes two HTTP endpoints.

---

## `GET /vehicles`

An [SSE (Server-Sent Events)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events) stream of vehicle state events in MBTA V3 JSON:API format.

**Content-Type:** `text/event-stream`

### Connection behavior

On connection, the client immediately receives a `reset` event containing all currently known vehicles. Subsequent events arrive as vehicles change state. The connection remains open until the client disconnects.

```
event: reset
data: [<vehicle>, <vehicle>, ...]

event: update
data: [<vehicle>, ...]

event: remove
data: [{"id": "vehicle-123"}, ...]
```

### Event types

| Event | Meaning |
|---|---|
| `reset` | Full snapshot of all currently tracked vehicles. Sent once on connection. |
| `update` | One or more vehicles changed state (position, trip, stop). Also used for new vehicles. |
| `remove` | One or more vehicles have been dropped from the feed. The `data` array contains only `{"id": "..."}` objects. |

### Vehicle resource shape

Each item in a `reset` or `update` data array is a JSON:API vehicle resource:

```json
{
  "id": "vehicle-123",
  "type": "vehicle",
  "attributes": {
    "current_status": "IN_TRANSIT_TO",
    "current_stop_sequence": 14,
    "direction_id": 0,
    "label": "1234",
    "latitude": 41.8255,
    "longitude": -71.4128,
    "bearing": 270.0,
    "speed": null,
    "updated_at": "2024-06-01T12:00:00+00:00",
    "occupancy_status": null,
    "provenance": "observed",
    "confidence": "confirmed"
  },
  "relationships": {
    "trip": {"data": {"id": "trip-456", "type": "trip"}},
    "route": {"data": {"id": "route-1", "type": "route"}},
    "stop": {"data": {"id": "stop-789", "type": "stop"}}
  }
}
```

### Attributes

| Field | Type | Description |
|---|---|---|
| `current_status` | string | `"INCOMING_AT"`, `"STOPPED_AT"`, or `"IN_TRANSIT_TO"` |
| `current_stop_sequence` | int \| null | Stop sequence number from the current trip |
| `direction_id` | int \| null | `0` or `1` per GTFS direction |
| `label` | string \| null | Human-readable vehicle label (e.g. bus number) |
| `latitude` | float | WGS84 decimal degrees |
| `longitude` | float | WGS84 decimal degrees |
| `bearing` | float \| null | Heading in degrees (0-359), clockwise from north |
| `speed` | float \| null | Speed in meters per second |
| `updated_at` | ISO 8601 string | Timestamp of this event |
| `occupancy_status` | null | Always null; reserved for future use |
| `provenance` | string | How this event was produced (see below) |
| `confidence` | string | How certain nibble is about the trip assignment (see below) |

### Relationships

Each relationship's `data` is either a `{"id": "...", "type": "..."}` object or `null` if unknown.

| Relationship | Type | Description |
|---|---|---|
| `trip` | `"trip"` | The GTFS trip the vehicle is currently on |
| `route` | `"route"` | The route the vehicle is serving |
| `stop` | `"stop"` | The stop the vehicle is approaching or stopped at |

### `provenance` values

`provenance` describes how the event's data was produced:

| Value | Meaning |
|---|---|
| `"observed"` | Directly reported by the GTFS-RT feed |
| `"inferred"` | Position was observed, but trip/route/stop was carried forward from a prior poll (vehicle temporarily lost its `trip_id`) |
| `"interpolated"` | Synthetic event generated to fill a stop gap between two observed polls |
| `"manual"` | Trip assignment issued by an operator via the corrections API |

Consumers may display `"interpolated"` events differently (e.g. reduced opacity, dashed track line) to reflect that the position is nibble's estimate rather than a direct observation.

### `confidence` values

`confidence` describes how certain nibble is about the vehicle's current trip assignment:

| Value | Meaning |
|---|---|
| `"confirmed"` | Vehicle reported a `trip_id` in this poll |
| `"inferred"` | Vehicle's `trip_id` is carried forward from a prior poll; it may have changed trips |
| `"stale"` | Vehicle exceeded the stale threshold; a `remove` event follows in the same cycle |

In practice, `"stale"` events are immediately followed by a `remove` and should not reach display logic. Consumers may wish to treat `"inferred"` trips with reduced confidence when making routing or ETA calculations.

### Example: connecting with JavaScript

```javascript
const source = new EventSource("http://localhost:8080/vehicles");

source.addEventListener("reset", (e) => {
  const vehicles = JSON.parse(e.data);
  // replace all vehicles in your store
});

source.addEventListener("update", (e) => {
  const vehicles = JSON.parse(e.data);
  // upsert each vehicle
});

source.addEventListener("remove", (e) => {
  const removed = JSON.parse(e.data);
  // remove each vehicle by id
});
```

---

## `GET /health`

A JSON health check endpoint for use with load balancers and uptime monitors.

**Content-Type:** `application/json`

### Response

```json
{
  "status": "ok",
  "last_poll_time": "2024-06-01T12:00:05+00:00",
  "connected_clients": 3
}
```

| Field | Type | Description |
|---|---|---|
| `status` | string | Always `"ok"` while the process is running |
| `last_poll_time` | ISO 8601 string \| null | Timestamp of the last successful feed poll; `null` if no poll has completed yet |
| `connected_clients` | int | Number of currently connected SSE clients |

`last_poll_time` is only updated when the feed was fetched and events were broadcast. A stale `last_poll_time` (or `null`) indicates that the feed may be unreachable or returning errors. Check application logs for details.

---

## `POST /trip_assignments`

Create or replace a manual trip assignment for a vehicle. See the [manual trip assignment corrections](how-to/trip-assignment-corrections.md) guide for full details.

**Content-Type:** `application/json`

### Request body

```json
{ "vehicle_id": "BUS-42", "trip_id": "trip-123" }
```

### Response `200 OK`

```json
{ "vehicle_id": "BUS-42", "trip_id": "trip-123", "assigned_at": "2024-06-01T12:34:56.789012+00:00" }
```

Returns `422` if either field is missing or `trip_id` is not in the loaded static GTFS.

---

## `GET /trip_assignments`

List all currently active manual trip overrides.

**Content-Type:** `application/json`

### Response `200 OK`

```json
{
  "BUS-42": { "trip_id": "trip-123", "assigned_at": "2024-06-01T12:34:56.789012+00:00" }
}
```

---

## `DELETE /trip_assignments/{vehicle_id}`

Remove the manual override for a vehicle. No-op if none exists.

### Response

`204 No Content`
