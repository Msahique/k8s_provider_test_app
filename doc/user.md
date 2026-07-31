# User Guide

The provider app serves a web console and four APIs. This guide covers using both.

**Console:** `/` or `/ui` on port 8000 — e.g. http://127.0.0.1:8000/, or
`kubectl port-forward svc/fastapi-db-api 8000:8000` for a cluster deployment.

---

## 1. The console

A left sidebar switches between three views; the top bar shows live database
status and a light/dark toggle that remembers your choice.

### APIs

The full catalogue of endpoints this application exposes. Each card carries the
method, path, a description, an example request body and an example response.

- Search filters across paths, methods, summaries and descriptions.
- **Expand all** / **Collapse all** for a quick scan.
- **copy** on any sample copies it to the clipboard, ready to paste into curl or Postman.
- **OpenAPI ↗** opens FastAPI's interactive `/docs` page, where requests can be executed in the browser.

### Logs

Every inbound call, newest first, with four summary tiles above: total calls in
the buffer, successful, failed, and average latency with the age of the last call.

| Column | Meaning |
|---|---|
| Time (UTC) | When the request arrived |
| Caller | Origin address — the real consumer IP when the call came through a gateway |
| Method / Endpoint | What was called |
| Status | HTTP status; 4xx and 5xx in red |
| Latency | Server-side processing time |
| Request | First ~70 characters of the request body |
| Acknowledgement | First ~70 characters of what was returned |
| Sent to | Who received the acknowledgement |

Click any row to open a detail panel with the complete request body, the complete
acknowledgement, the user agent, content type, query parameters and a trace id —
each copyable. Esc closes it.

Filter by free text (searches the whole entry, including bodies), by status class,
or by method. **Live** polls every 5 seconds; untick it to freeze the view while
reading. **Clear** empties the log in memory and on disk — this cannot be undone.

Health-probe traffic is excluded so it does not drown real calls. The buffer holds
the most recent 500 entries; older ones are discarded.

### Inbox

Events received on `/incoming_message`, newest first, with tiles for total
messages, distinct senders, distinct event types and time since the last one.

Columns: Received, Sender, Event type, Room, Method, Source IP, Event payload,
Acknowledged. Filter by text, by event type, or by room — the dropdowns are built
from the data actually present.

Click a row for the event payload, the full envelope exactly as received, and the
acknowledgement returned.

Messages are persisted to `inbox.json`, so they survive a restart. The buffer
holds the most recent 500.

---

## 2. Calling the APIs

Examples use PowerShell. Note that `curl` is an alias for `Invoke-WebRequest` in
PowerShell — use `Invoke-RestMethod`, or `curl.exe` for real curl.

```powershell
$u = "http://localhost:8000"
```

### `GET /health`

```powershell
Invoke-RestMethod "$u/health"
```

```json
{ "status": "healthy", "database": "connected", "timestamp": "2026-07-29T10:15:00+00:00" }
```

`database` reports `connected`, or `error: <detail>` when MySQL is unreachable.
The Kubernetes readiness and liveness probes use this endpoint, so a database
outage takes the pod out of service.

### `GET /hello`

A static greeting — the quickest way to confirm traffic reaches the app through
the gateway.

### `POST /qry` — run SQL and get real rows

```powershell
Invoke-RestMethod "$u/qry" -Method Post -ContentType application/json `
  -Body '{"qry":"SELECT id, name FROM users LIMIT 10"}' | ConvertTo-Json -Depth 5
```

Anything that produces a result set — `SELECT`, `SHOW`, `DESCRIBE`, `WITH`,
`EXPLAIN`, `CALL` — returns the actual rows:

```json
{
  "success": true, "result_set": true, "rows": 2,
  "columns": ["id", "name"],
  "data": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
  "timestamp": "2026-07-29T10:15:00+00:00"
}
```

Statements without a result set return counts instead:

```json
{ "success": true, "result_set": false, "affected_rows": 3,
  "last_insert_id": 77, "message": "Query executed successfully." }
```

`DATETIME`, `DECIMAL` and `BLOB` values are converted to JSON-safe forms
(ISO timestamps, numbers, decoded text). An empty query returns 400; a SQL error
returns 500 with the database's message in `detail`.

> This endpoint runs whatever SQL it is given, unauthenticated, as the configured
> user (`root` by default). Treat access to port 8000 as full database access.

### `POST /incoming_message` — send an event

```powershell
Invoke-RestMethod "$u/incoming_message" -Method Post -ContentType application/json -Body '{
  "eventType": "demo-event",
  "event": { "patientId": 42, "status": "admitted" },
  "room": "default-room"
}'
```

| Field | Required | Notes |
|---|---|---|
| `eventType` | recommended | Names the event. Falls back to `event_type`, `type`, `request_type`, `message_type`, then `"message"` |
| `event` | recommended | The payload object; stored and displayed separately from the envelope |
| `room` | optional | Defaults to `default-room`. `roomId` also accepted |
| `sender` | optional | Also read from `from`, `consumer`, `event.sender`, or the `X-Sender` header; falls back to the caller's IP |

Response:

```json
{
  "success": true,
  "message_id": "9f21c0d5",
  "received_at": "2026-07-29T10:15:00+00:00",
  "sender": "10.244.0.17",
  "eventType": "demo-event",
  "room": "default-room",
  "event": { "patientId": 42, "status": "admitted" },
  "received_message": { "eventType": "demo-event", "event": {"patientId": 42, "status": "admitted"}, "room": "default-room" },
  "acknowledgement": "Event 'demo-event' received on room 'default-room' and stored in inbox."
}
```

The envelope is echoed back in `received_message` so the sender can confirm
exactly what arrived. An empty body returns 400. Other JSON shapes are accepted
and stored whole rather than rejected.

Set a sender name explicitly with a header if your producer has no `sender` field:

```powershell
Invoke-RestMethod "$u/incoming_message" -Method Post -ContentType application/json `
  -Headers @{ "X-Sender" = "consumer_app" } -Body '{"eventType":"demo-event","event":{"id":1}}'
```

### Read-model endpoints

`GET /api/endpoints`, `GET /api/logs?limit=N`, `GET /api/inbox?limit=N` return
the same data the console shows, for scripting. `DELETE /api/logs` clears the log.

---

## 3. Troubleshooting

### The console loads but every tab is empty

Shows *0 of 0 endpoints*, empty Logs and Inbox, and the browser console is full of
404s for `/api/logs?limit=200` and `/api/inbox?limit=200`.

**Cause:** the app is being served under a reverse-proxy path prefix such as
`http://106.51.108.71:9091/app1/`, but the page requests data from the server root
(`/api/logs`) instead of the prefix (`/app1/api/logs`). The proxy only routes
`/app1/*`, so every data call 404s. This is an open bug, not a misconfiguration on
your side.

**Workarounds until it is fixed:**

- Reach the app without a prefix — `kubectl port-forward svc/fastapi-db-api 8000:8000`, then http://localhost:8000/.
- Or give the app its own hostname/port at the proxy so it is mounted at `/`.
- The APIs themselves work fine through the prefix; only the console's data calls
  are affected. `POST /app1/incoming_message` and `POST /app1/qry` behave normally.

### Header says "Database unavailable"

`/health` could not connect. Check `IM_DB_HOST` / `IM_DB_PORT` in the ConfigMap
point at a reachable MySQL, that the MySQL pod is running, and that `IM_DB_NAME`
names a database that exists. `GET /health` returns the driver's error message in
the `database` field. `/qry` will fail while this persists; the Logs and Inbox
tabs are unaffected.

### Inbox is empty after a restart

Messages persist to `inbox.json` under `IM_DATA_DIR`. In the cluster this is
`/app/data`, backed by a PVC. If the deployment was applied without the volume,
the file lives on the container's writable layer and is lost when the pod is
replaced. Check the pod log at startup — it prints the data directory and how many
entries it reloaded.

### An old message shows a blank Event type or Room

Entries written before the event-envelope parser existed have no `eventType` or
`room`. They fall back to their original `request_type` and display `—` for room.
New messages are unaffected.

### Only the most recent entries are visible

Both buffers keep the newest 500 entries (`IM_LOG_MAX`, `IM_INBOX_MAX`). Raise
them in the ConfigMap if you need deeper history, and remember the whole buffer is
rewritten to disk on every write.
