# Test Guide

Scope: the provider application in [`app.py`](../app.py) — four business
endpoints, three read-model endpoints, the web console, and the persistence layer.

---

## 1. Setting up

### Without a database (fastest, covers everything except `/qry` results)

```powershell
cd c:\Users\Sahique\Desktop\new_workspace\2026\IM_new\Test_app_4_cluster3
..\.venv\Scripts\python.exe -m uvicorn app:app --port 8000
```

`/health` will report `database: error: …` and `/qry` will return 500. Logs,
Inbox, console and validation cases all still work.

### With a stubbed database (covers `/qry` result handling)

Use `TestClient` and replace the connection factory. Set `IM_DATA_DIR` to a temp
directory **before importing** `app`, or the run writes into the source folder.

```python
import os, tempfile, sys
os.environ["IM_DATA_DIR"] = tempfile.mkdtemp()
sys.path.insert(0, r"...\Test_app_4_cluster3")
import app as A
from fastapi.testclient import TestClient

class FakeCursor:
    description = [("id",), ("name",)]        # None => "no result set" branch
    rowcount, lastrowid = 2, 9
    def execute(self, sql): pass
    def fetchall(self):
        import datetime, decimal
        return [{"id": 1, "name": "alice",
                 "at": datetime.datetime(2026, 7, 1), "amt": decimal.Decimal("3.5")}]
    def __enter__(self): return self
    def __exit__(self, *a): return False

class FakeConn:
    def cursor(self): return FakeCursor()
    def close(self): pass

A.get_connection = lambda: FakeConn()
c = TestClient(A.app)
```

### Against the cluster

```powershell
kubectl port-forward svc/fastapi-db-api 8000:8000
```

Note `test_app.yaml` currently has a malformed `image:` key (line 58) and will not
apply until that is corrected — see [developer.md](developer.md#7-deploying-to-kubernetes).

---

## 2. Test matrix

`$u = "http://localhost:8000"`. PowerShell aliases `curl` to `Invoke-WebRequest`;
use `Invoke-RestMethod` or `curl.exe`.

### `GET /health`

| # | Case | Expected |
|---|---|---|
| H1 | DB reachable | 200, `database: "connected"`, ISO UTC `timestamp` |
| H2 | DB down (stop MySQL / point `IM_DB_HOST` at a dead host) | 200 with `database: "error: …"` — **not** a 5xx; the probe distinguishes via the field, and the pod goes NotReady |
| H3 | Called repeatedly | No entries appear in the Logs tab (excluded by default) |
| H4 | `IM_LOG_HEALTH=true`, restart, call again | Entries now appear in Logs |

### `GET /hello`

| # | Case | Expected |
|---|---|---|
| L1 | Normal call | 200, `application`, `message`, `timestamp` |
| L2 | After call | Appears in Logs with method GET, status 200, `Request` column `—` |

### `POST /qry`

| # | Body | Expected |
|---|---|---|
| Q1 | `{"qry":"SELECT id, name FROM users LIMIT 10"}` | 200, `result_set: true`, `columns` array, `data` with real rows, `rows` = row count |
| Q2 | `{"qry":"SHOW DATABASES"}` | 200 with **rows**, not a count — verifies the `cursor.description` check rather than a `startswith("select")` test |
| Q3 | `{"qry":"DESCRIBE users"}` | 200 with rows |
| Q4 | `{"qry":"INSERT INTO users (name) VALUES ('x')"}` | 200, `result_set: false`, `affected_rows`, `last_insert_id` |
| Q5 | `{"qry":"UPDATE users SET name='y' WHERE id=1"}` | 200, `affected_rows` reflects matched rows |
| Q6 | `{"qry":"   "}` | **400**, `detail: "qry must not be empty."` |
| Q7 | `{}` (field missing) | 422 from Pydantic validation |
| Q8 | `{"qry":"SELECT * FROM no_such_table"}` | 500, `detail` carries the MySQL error text |
| Q9 | Table with `DATETIME`, `DECIMAL`, `BLOB` columns | 200 — timestamps as ISO strings, decimals as numbers, blobs decoded or rendered `<N bytes>`; **no** 500 from JSON serialisation |
| Q10 | Any of the above | A matching Logs entry with the SQL in `Request` and the result in `Acknowledgement` |

Q9 is the regression case for the row-serialisation fix — a raw `DATETIME` in a
result set used to crash the response.

### `POST /incoming_message`

| # | Body / headers | Expected |
|---|---|---|
| M1 | `{"eventType":"demo-event","event":{"patientId":42,"status":"admitted"},"room":"default-room"}` | 200; `eventType`, `room`, `event` echoed; `received_message` equals the envelope; ack reads `Event 'demo-event' received on room 'default-room' and stored in inbox.` |
| M2 | `{"event":{"x":1}}` | 200, `eventType` = `"message"`, `room` = `"default-room"`, `sender` = caller IP |
| M3 | `{"eventType":"e","event":{},"roomId":"ward-3"}` | 200, `room` = `"ward-3"` |
| M4 | `{"sender":"consumer_app","type":"notification","message":"order dispatched"}` | 200 — legacy shape accepted, `eventType` = `"notification"`, `sender` = `"consumer_app"` |
| M5 | `{"eventType":"e","event":{"sender":"svc-a"}}` | 200, `sender` = `"svc-a"` (read from inside `event`) |
| M6 | Header `X-Sender: consumer_app`, body without a sender field | 200, `sender` = `"consumer_app"` |
| M7 | Empty body | **400**, `detail: "Request body must not be empty."` |
| M8 | `[1,2,3]` (JSON array) | 200 — tolerated, stored whole in `message`, `eventType` = `"message"` |
| M9 | Malformed JSON (`{"a":`) | 200 — stored as text; the parser must not 500 |
| M10 | Any accepted message | Appears in Inbox within 5s (Live on) and in Logs; `message_id` matches the response |

### Read-model endpoints

| # | Case | Expected |
|---|---|---|
| R1 | `GET /api/endpoints` | 200, `count` = 8, each entry has `method`, `path`, `summary`, `description`, `request`, `response` |
| R2 | `GET /api/logs` | 200, newest first, `total` ≥ `count` |
| R3 | `GET /api/logs?limit=1` | Exactly one entry — the most recent |
| R4 | `GET /api/inbox?limit=0` | 200, empty list, no error |
| R5 | `DELETE /api/logs` | 200 with `cleared` = prior count; a following GET returns 0 |
| R6 | Any of R1–R5 | **No** self-entries appear in the log (console paths are excluded) |

### Console

| # | Case | Expected |
|---|---|---|
| C1 | Load `/` and `/ui` | Both render the same page |
| C2 | Sidebar counts | Match the APIs / Logs / Inbox totals |
| C3 | Click a Logs row | Drawer opens with full request body and acknowledgement; Esc closes |
| C4 | Logs filters | Text, status class and method filters narrow the rows; the counter updates |
| C5 | Inbox event-type and room dropdowns | Populated from live data; selecting one filters |
| C6 | Live toggle off | Rows stop refreshing; on, they resume within 5s |
| C7 | Clear button | Confirmation prompt; on accept the table empties and `api_logs.json` is truncated |
| C8 | Theme toggle | Switches light/dark and survives a reload |
| C9 | Scroll a long log table | The header row stays pinned at the top of the card and never overlaps data rows (regression: it previously rendered between rows) |
| C10 | Narrow window to ~400px | Sidebar becomes a top bar; no horizontal scrolling of the page body — only inside the table |
| C11 | Send a message with `<script>alert(1)</script>` in a field | Rendered as literal text in the table and drawer; no dialog, no broken markup |

C11 is the XSS regression check — every interpolated value goes through `esc()`.

### Persistence

| # | Case | Expected |
|---|---|---|
| P1 | Send messages, restart the process | Inbox and Logs still populated; startup log prints `loaded N log entries, M inbox messages` |
| P2 | `IM_DATA_DIR` unset locally | `inbox.json` and `api_logs.json` created next to `app.py` |
| P3 | In-cluster, `kubectl delete pod -l app=fastapi-db-api` | After the new pod is Ready, the inbox is intact (PVC) |
| P4 | Read-only or unwritable `IM_DATA_DIR` | App keeps serving; a `[store] could not write …` line appears in the pod log; data is memory-only |
| P5 | Corrupt `inbox.json` (truncate it mid-file), restart | App starts, logs the load failure, inbox begins empty — no crash loop |
| P6 | `IM_INBOX_MAX=5`, send 7 messages | Only the newest 5 are retained in memory and on disk |

### Middleware behaviour

| # | Case | Expected |
|---|---|---|
| A1 | Any POST | Response body received by the client is byte-identical to what the endpoint returned (regression: draining the stream without rebuilding truncated it to empty) |
| A2 | Request with `X-Forwarded-For: 203.0.113.9, 10.0.0.1` | Logged `caller` = `203.0.113.9` |
| A3 | No `X-Forwarded-For` | `caller` = peer IP |
| A4 | Body larger than `IM_BODY_MAX` (default 8000 chars) | Endpoint processes it in full; log entry stores truncated text ending `...[truncated]` |
| A5 | Error responses (400/500) | Logged with the correct status and the error body as the acknowledgement |
| A6 | Latency | `duration_ms` is a plausible positive number |

---

## 3. Known issues to verify, not report

| Issue | Symptom | Status |
|---|---|---|
| Reverse-proxy path prefix | Console served at `http://host:9091/app1/` loads, but all tabs show zero and the browser console logs 404s for `/api/logs` and `/api/inbox` — the JS requests the server root instead of the `/app1` prefix | Open. Test the console via `port-forward` or a root-mounted host until fixed |
| Malformed `image:` key | `kubectl apply -f test_app.yaml` fails on the Deployment | Open. Fix line 58 to `image: msahique/helloworld_test_k8s_app:v2` before cluster testing |
| Stale image after rebuild | New code not running despite a rebuild | Expected — `imagePullPolicy: IfNotPresent` requires a new tag per release |

---

## 4. Regression checklist

Run before every release:

1. Q2 — `SHOW DATABASES` returns rows, not a count
2. Q9 — a result set containing `DATETIME` / `DECIMAL` / `BLOB` serialises
3. Q6, M7 — empty inputs return 400, not 500
4. M1 — the documented envelope round-trips with the correct ack text
5. M4 — the legacy `{sender, type, message}` shape still parses
6. A1 — POST response bodies arrive intact through the middleware
7. C9 — sticky table header does not land between rows
8. C11 — script tags in message fields render as text
9. P1 — restart preserves both stores
10. R6 — console polling does not appear in its own log
