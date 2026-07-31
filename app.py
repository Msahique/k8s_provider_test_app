from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel
from datetime import datetime, date, time as dtime, timezone
from decimal import Decimal
from collections import deque
from threading import Lock
import json
import os
import uuid
import pymysql

app = FastAPI(
    title="Simple FastAPI Database API",
    version="2.0"
)


# ----------------------------------------------------
# Database Configuration
# ----------------------------------------------------
DB_HOST = os.getenv("IM_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("IM_DB_PORT", "3306"))
DB_USER = os.getenv("IM_DB_USER", "root")
DB_PASS = os.getenv("IM_DB_PASS", "root")
DB_NAME = os.getenv("IM_DB_NAME", "ca_db")


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME if DB_NAME else None,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )


# ----------------------------------------------------
# Storage Configuration
# ----------------------------------------------------
DATA_DIR = os.getenv("IM_DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
INBOX_FILE = os.path.join(DATA_DIR, "inbox.json")
LOG_FILE = os.path.join(DATA_DIR, "api_logs.json")

LOG_MAX = int(os.getenv("IM_LOG_MAX", "500"))
INBOX_MAX = int(os.getenv("IM_INBOX_MAX", "500"))
BODY_MAX = int(os.getenv("IM_BODY_MAX", "8000"))          # chars kept per body
LOG_HEALTH = os.getenv("IM_LOG_HEALTH", "false").lower() == "true"
DEFAULT_ROOM = os.getenv("IM_DEFAULT_ROOM", "default-room")

# Paths that serve the UI itself - logging them would drown the real traffic.
NO_LOG_PATHS = {
    "/", "/ui", "/favicon.ico",
    "/api/endpoints", "/api/logs", "/api/inbox",
    "/docs", "/redoc", "/openapi.json",
}

_logs = deque(maxlen=LOG_MAX)
_inbox = deque(maxlen=INBOX_MAX)
_store_lock = Lock()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def jsonable(value):
    """Make DB / request values safe for json.dumps()."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date, dtime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return f"<{len(value)} bytes>"
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    return str(value)


def load_store(path, target, limit):
    """Reload persisted entries on startup; missing/corrupt file is not fatal."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            entries = json.load(fh)
        if isinstance(entries, list):
            target.extend(entries[-limit:])
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[store] could not load {path}: {e}")


def save_store(path, target):
    """Best effort persistence - a read-only filesystem keeps the app running."""
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(list(target), fh, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"[store] could not write {path}: {e}")


def decode_body(raw):
    """Return a JSON-parsed body when possible, else trimmed text."""
    if not raw:
        return None
    text = raw.decode("utf-8", errors="replace")
    truncated = len(text) > BODY_MAX
    if truncated:
        text = text[:BODY_MAX] + " ...[truncated]"
    if not truncated:
        try:
            return json.loads(text)
        except ValueError:
            pass
    return text


def caller_of(request: Request):
    """Best guess at who called us, honouring proxy / gateway headers."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


@app.on_event("startup")
def startup():
    load_store(LOG_FILE, _logs, LOG_MAX)
    load_store(INBOX_FILE, _inbox, INBOX_MAX)
    print(f"[startup] data dir: {DATA_DIR}")
    print(f"[startup] loaded {len(_logs)} log entries, {len(_inbox)} inbox messages")


# ----------------------------------------------------
# Request / response audit log
# ----------------------------------------------------
def record_log(entry):
    with _store_lock:
        _logs.append(entry)
        snapshot = list(_logs)
    save_store(LOG_FILE, snapshot)


@app.middleware("http")
async def audit_middleware(request: Request, call_next):
    path = request.url.path
    skip = path in NO_LOG_PATHS or (path == "/health" and not LOG_HEALTH)

    if skip:
        return await call_next(request)

    started = datetime.now(timezone.utc)
    raw_request = await request.body()

    response = await call_next(request)

    # Drain the response so the acknowledgement can be logged, then rebuild it.
    chunks = [chunk async for chunk in response.body_iterator]
    raw_response = b"".join(chunks)

    entry = {
        "id": uuid.uuid4().hex[:12],
        "timestamp": started.isoformat(),
        "caller": caller_of(request),
        "caller_port": request.client.port if request.client else None,
        "user_agent": request.headers.get("user-agent", ""),
        "method": request.method,
        "endpoint": path,
        "query_params": dict(request.query_params) or None,
        "content_type": request.headers.get("content-type", ""),
        "request_body": decode_body(raw_request),
        "status_code": response.status_code,
        "acknowledgement": decode_body(raw_response),
        "responded_to": caller_of(request),
        "duration_ms": round(
            (datetime.now(timezone.utc) - started).total_seconds() * 1000, 2
        ),
    }
    await run_in_threadpool(record_log, entry)

    return Response(
        content=raw_response,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type,
    )


# ----------------------------------------------------
# Models
# ----------------------------------------------------
class QueryRequest(BaseModel):
    qry: str


# ----------------------------------------------------
# API catalogue (drives the "APIs" tab)
# ----------------------------------------------------
API_CATALOG = [
    {
        "method": "GET",
        "path": "/health",
        "summary": "Liveness / readiness probe",
        "description": "Opens and closes a MySQL connection and reports whether the "
                       "database is reachable. Used by the Kubernetes readiness and "
                       "liveness probes. Not written to the audit log unless "
                       "IM_LOG_HEALTH=true.",
        "request": None,
        "response": {
            "status": "healthy",
            "database": "connected",
            "timestamp": "2026-07-29T10:15:00+00:00",
        },
    },
    {
        "method": "GET",
        "path": "/hello",
        "summary": "Simple greeting",
        "description": "Static response used to confirm the provider app is serving "
                       "traffic through the gateway.",
        "request": None,
        "response": {
            "application": "Simple FastAPI Database API",
            "message": "Hello from FastAPI!",
            "timestamp": "2026-07-29T10:15:00+00:00",
        },
    },
    {
        "method": "POST",
        "path": "/qry",
        "summary": "Execute a SQL query and return real rows",
        "description": "Runs the supplied SQL against MySQL. Any statement that "
                       "produces a result set (SELECT, SHOW, DESCRIBE, WITH, CALL, "
                       "EXPLAIN) returns the actual rows plus the column list. "
                       "Statements without a result set return the affected row count.",
        "request": {"qry": "SELECT id, name FROM users LIMIT 10"},
        "response": {
            "success": True,
            "result_set": True,
            "rows": 2,
            "columns": ["id", "name"],
            "data": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        },
    },
    {
        "method": "POST",
        "path": "/incoming_message",
        "summary": "Receive an event from a consumer",
        "description": "Accepts the event envelope shown below, echoes it back in the "
                       "acknowledgement and stores it in inbox.json. eventType names "
                       "the event, event carries the payload, room names the channel "
                       "it was published on (defaults to 'default-room'). Other JSON "
                       "shapes are still accepted: type / request_type / message_type "
                       "are read as fallbacks for eventType. Sender comes from the body "
                       "(sender / from / consumer), the X-Sender header, or the caller IP.",
        "request": {
            "eventType": "demo-event",
            "event": {"patientId": 42, "status": "admitted"},
            "room": "default-room",
        },
        "response": {
            "success": True,
            "message_id": "9f21c0d5",
            "received_at": "2026-07-29T10:15:00+00:00",
            "sender": "10.244.0.17",
            "eventType": "demo-event",
            "room": "default-room",
            "event": {"patientId": 42, "status": "admitted"},
            "received_message": {
                "eventType": "demo-event",
                "event": {"patientId": 42, "status": "admitted"},
                "room": "default-room",
            },
            "acknowledgement": "Event 'demo-event' received on room 'default-room' "
                               "and stored in inbox.",
        },
    },
    {
        "method": "GET",
        "path": "/api/endpoints",
        "summary": "This API catalogue as JSON",
        "description": "Backs the APIs tab of the dashboard.",
        "request": None,
        "response": {"count": 8, "endpoints": ["..."]},
    },
    {
        "method": "GET",
        "path": "/api/logs",
        "summary": "Audit log of every API call",
        "description": "Newest first. Each entry holds the caller, timestamp, endpoint, "
                       "HTTP method, request body, status code, the acknowledgement "
                       "that was sent back and who it was sent to. Optional ?limit=N.",
        "request": None,
        "response": {"count": 1, "logs": ["..."]},
    },
    {
        "method": "GET",
        "path": "/api/inbox",
        "summary": "Events received on /incoming_message",
        "description": "Newest first, read from the in-memory ring buffer that is "
                       "mirrored to inbox.json. Each entry carries the sender, receive "
                       "time, eventType, room, the event payload and the ack returned. "
                       "Optional ?limit=N.",
        "request": None,
        "response": {"count": 1, "messages": ["..."]},
    },
    {
        "method": "DELETE",
        "path": "/api/logs",
        "summary": "Clear the audit log",
        "description": "Empties the in-memory log and truncates api_logs.json.",
        "request": None,
        "response": {"success": True, "cleared": 42},
    },
]


# ----------------------------------------------------
# Health
# ----------------------------------------------------
@app.get("/health")
def health():
    try:
        conn = get_connection()
        conn.close()

        db_status = "connected"

    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status": "healthy",
        "database": db_status,
        "timestamp": utc_now()
    }


# ----------------------------------------------------
# Hello
# ----------------------------------------------------
@app.get("/hello")
def hello():
    return {
        "application": "Simple FastAPI Database API",
        "message": "Hello from FastAPI!",
        "timestamp": utc_now()
    }


# ----------------------------------------------------
# Execute Query - returns the real rows from the database
# ----------------------------------------------------
@app.post("/qry")
def execute_query(req: QueryRequest):

    sql = req.qry.strip()

    if not sql:
        raise HTTPException(status_code=400, detail="qry must not be empty.")

    conn = None

    try:
        conn = get_connection()

        with conn.cursor() as cursor:

            cursor.execute(sql)

            # cursor.description is set for anything that returns a result set:
            # SELECT, SHOW, DESCRIBE, WITH, EXPLAIN, CALL ...
            if cursor.description:

                rows = jsonable(cursor.fetchall())
                columns = [col[0] for col in cursor.description]

                return {
                    "success": True,
                    "result_set": True,
                    "rows": len(rows),
                    "columns": columns,
                    "data": rows,
                    "timestamp": utc_now()
                }

            affected = cursor.rowcount

            return {
                "success": True,
                "result_set": False,
                "affected_rows": affected,
                "last_insert_id": cursor.lastrowid,
                "message": "Query executed successfully.",
                "timestamp": utc_now()
            }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# ----------------------------------------------------
# Incoming Message
# ----------------------------------------------------
def record_message(entry):
    with _store_lock:
        _inbox.append(entry)
        snapshot = list(_inbox)
    save_store(INBOX_FILE, snapshot)


@app.post("/incoming_message")
async def incoming_message(request: Request):

    raw = await request.body()
    payload = decode_body(raw)

    if payload is None:
        raise HTTPException(status_code=400, detail="Request body must not be empty.")

    body = payload if isinstance(payload, dict) else {}

    # Expected shape:
    #   {"eventType": "demo-event",
    #    "event": {"patientId": 42, "status": "admitted"},
    #    "room": "default-room"}
    # Other shapes are still accepted so older//ad-hoc producers keep working.
    event_type = (
        body.get("eventType")
        or body.get("event_type")
        or body.get("type")
        or body.get("request_type")
        or body.get("message_type")
        or "message"
    )
    event = body.get("event")
    room = body.get("room") or body.get("roomId") or DEFAULT_ROOM

    sender = (
        body.get("sender")
        or body.get("from")
        or body.get("consumer")
        or (event.get("sender") if isinstance(event, dict) else None)
        or request.headers.get("x-sender")
        or caller_of(request)
    )

    received_at = utc_now()
    message_id = uuid.uuid4().hex[:8]
    acknowledgement = f"Event '{event_type}' received on room '{room}' and stored in inbox."

    entry = {
        "message_id": message_id,
        "received_at": received_at,
        "sender": str(sender),
        "event_type": str(event_type),
        "request_type": str(event_type),      # kept for older inbox readers
        "room": str(room),
        "event": jsonable(event),
        "http_method": request.method,
        "endpoint": request.url.path,
        "source_ip": caller_of(request),
        "content_type": request.headers.get("content-type", ""),
        "user_agent": request.headers.get("user-agent", ""),
        "message": jsonable(payload),
        "acknowledgement": acknowledgement,
        "acknowledged_to": str(sender),
    }

    await run_in_threadpool(record_message, entry)

    return {
        "success": True,
        "message_id": message_id,
        "received_at": received_at,
        "sender": entry["sender"],
        "eventType": entry["event_type"],
        "room": entry["room"],
        "event": entry["event"],
        "received_message": entry["message"],
        "acknowledgement": acknowledgement,
    }


# ----------------------------------------------------
# Dashboard data endpoints
# ----------------------------------------------------
@app.get("/api/endpoints")
def list_endpoints():
    return {"count": len(API_CATALOG), "endpoints": API_CATALOG}


@app.get("/api/logs")
def list_logs(limit: int = 200):
    with _store_lock:
        entries = list(_logs)
    entries.reverse()
    entries = entries[:max(limit, 0)]
    return {"count": len(entries), "total": len(_logs), "logs": entries}


@app.delete("/api/logs")
def clear_logs():
    with _store_lock:
        cleared = len(_logs)
        _logs.clear()
    save_store(LOG_FILE, [])
    return {"success": True, "cleared": cleared}


@app.get("/api/inbox")
def list_inbox(limit: int = 200):
    with _store_lock:
        entries = list(_inbox)
    entries.reverse()
    entries = entries[:max(limit, 0)]
    return {"count": len(entries), "total": len(_inbox), "messages": entries}


# ----------------------------------------------------
# Dashboard (served inline so the image stays a single file)
# ----------------------------------------------------
DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Provider Console &middot; Simple FastAPI Database API</title>
<style>
  :root {
    --bg:        #f6f7f9;
    --surface:   #ffffff;
    --surface-2: #f2f4f7;
    --surface-3: #e9edf2;
    --line:      #dfe4ea;
    --line-soft: #eaeef3;
    --ink:       #14202e;
    --ink-2:     #4a5b6e;
    --ink-3:     #7b8a9c;
    --accent:    #1f5fbf;
    --accent-sf: #e8f0fd;
    --ok:        #1a7f4b;
    --ok-sf:     #e4f4ea;
    --warn:      #a8630a;
    --warn-sf:   #fdf1de;
    --err:       #b32d28;
    --err-sf:    #fbe9e8;
    --shadow:    0 1px 2px rgba(17,32,48,.05), 0 4px 12px rgba(17,32,48,.05);
    --radius:    8px;
    --mono: ui-monospace, "SF Mono", "Cascadia Mono", "Segoe UI Mono", Consolas, monospace;
  }
  :root[data-theme="dark"], html.dark {
    --bg:        #0d1117;
    --surface:   #151b23;
    --surface-2: #1b232d;
    --surface-3: #232d39;
    --line:      #2a333f;
    --line-soft: #212a35;
    --ink:       #e7edf4;
    --ink-2:     #a9b6c6;
    --ink-3:     #78889b;
    --accent:    #5fa2f5;
    --accent-sf: #17293f;
    --ok:        #4cc38a;
    --ok-sf:     #14301f;
    --warn:      #e0a33c;
    --warn-sf:   #33260f;
    --err:       #f07a72;
    --err-sf:    #3a1b19;
    --shadow:    0 1px 2px rgba(0,0,0,.3), 0 4px 14px rgba(0,0,0,.25);
  }

  * { box-sizing: border-box; }
  html, body { height: 100%; }
  body {
    margin: 0; background: var(--bg); color: var(--ink);
    font: 14px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
          "Helvetica Neue", Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
  }
  ::selection { background: var(--accent-sf); }

  /* ---------- shell ---------- */
  .shell { display: grid; grid-template-columns: 232px 1fr; min-height: 100vh; }

  aside {
    background: var(--surface); border-right: 1px solid var(--line);
    display: flex; flex-direction: column; position: sticky; top: 0; height: 100vh;
  }
  .brand { padding: 20px 18px 18px; border-bottom: 1px solid var(--line-soft); }
  .brand .mark {
    width: 30px; height: 30px; border-radius: 7px; background: var(--accent);
    color: #fff; display: grid; place-items: center; font-weight: 700; font-size: 13px;
    letter-spacing: -.3px; margin-bottom: 10px;
  }
  .brand .name { font-weight: 650; font-size: 14px; letter-spacing: -.15px; }
  .brand .role { font-size: 11.5px; color: var(--ink-3); margin-top: 2px; }

  .nav { padding: 12px 10px; display: flex; flex-direction: column; gap: 2px; }
  .nav .sec {
    font-size: 10.5px; text-transform: uppercase; letter-spacing: .8px;
    color: var(--ink-3); padding: 10px 10px 6px; font-weight: 600;
  }
  .nav button {
    display: flex; align-items: center; gap: 10px; width: 100%; text-align: left;
    background: none; border: 0; border-radius: 6px; padding: 8px 10px;
    color: var(--ink-2); font: inherit; font-size: 13.5px; cursor: pointer;
  }
  .nav button:hover { background: var(--surface-2); color: var(--ink); }
  .nav button.active { background: var(--accent-sf); color: var(--accent); font-weight: 600; }
  .nav button .ico { width: 15px; text-align: center; opacity: .85; font-size: 13px; }
  .nav button .badge {
    margin-left: auto; font-size: 11px; font-variant-numeric: tabular-nums;
    background: var(--surface-3); color: var(--ink-2); padding: 1px 7px; border-radius: 999px;
  }
  .nav button.active .badge { background: var(--accent); color: #fff; }

  .side-foot { margin-top: auto; padding: 14px 16px; border-top: 1px solid var(--line-soft); }
  .side-foot .kv { display: flex; justify-content: space-between; gap: 8px; font-size: 11.5px; padding: 3px 0; }
  .side-foot .kv span:first-child { color: var(--ink-3); }
  .side-foot .kv span:last-child { color: var(--ink-2); font-family: var(--mono); }

  /* ---------- topbar ---------- */
  .main { display: flex; flex-direction: column; min-width: 0; }
  .topbar {
    display: flex; align-items: center; gap: 14px; padding: 0 26px; height: 60px;
    background: var(--surface); border-bottom: 1px solid var(--line);
    position: sticky; top: 0; z-index: 20;
  }
  .topbar h1 { margin: 0; font-size: 15.5px; font-weight: 650; letter-spacing: -.2px; }
  .topbar .sub { font-size: 12px; color: var(--ink-3); margin-top: 1px; }
  .grow { flex: 1; }

  .status {
    display: inline-flex; align-items: center; gap: 7px; font-size: 12.5px;
    padding: 5px 11px; border-radius: 999px; border: 1px solid var(--line);
    background: var(--surface-2); color: var(--ink-2); white-space: nowrap;
  }
  .status .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--ink-3); }
  .status.ok    { color: var(--ok);   border-color: transparent; background: var(--ok-sf); }
  .status.ok .dot   { background: var(--ok); box-shadow: 0 0 0 3px color-mix(in srgb, var(--ok) 22%, transparent); }
  .status.err   { color: var(--err);  border-color: transparent; background: var(--err-sf); }
  .status.err .dot  { background: var(--err); }

  .iconbtn {
    width: 32px; height: 32px; border-radius: 6px; border: 1px solid var(--line);
    background: var(--surface); color: var(--ink-2); cursor: pointer; font-size: 14px;
    display: grid; place-items: center;
  }
  .iconbtn:hover { border-color: var(--accent); color: var(--accent); }

  /* ---------- content ---------- */
  .content { padding: 22px 26px 64px; max-width: 1560px; width: 100%; }
  .panel { display: none; }
  .panel.show { display: block; animation: fade .18s ease; }
  @keyframes fade { from { opacity: 0; transform: translateY(3px); } to { opacity: 1; transform: none; } }

  .stats { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 12px; margin-bottom: 20px; }
  .stat {
    background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius);
    padding: 14px 16px; box-shadow: var(--shadow);
  }
  .stat .t { font-size: 11px; text-transform: uppercase; letter-spacing: .7px; color: var(--ink-3); font-weight: 600; }
  .stat .v { font-size: 23px; font-weight: 640; letter-spacing: -.5px; margin-top: 6px; font-variant-numeric: tabular-nums; }
  .stat .m { font-size: 11.5px; color: var(--ink-3); margin-top: 2px; font-family: var(--mono); }
  .stat .v.ok { color: var(--ok); } .stat .v.err { color: var(--err); }

  .toolbar { display: flex; align-items: center; gap: 9px; margin-bottom: 14px; flex-wrap: wrap; }
  .search { position: relative; }
  .search svg { position: absolute; left: 10px; top: 50%; transform: translateY(-50%); opacity: .5; }
  input[type=search] {
    background: var(--surface); border: 1px solid var(--line); color: var(--ink);
    padding: 8px 12px 8px 31px; border-radius: 7px; min-width: 300px; font: inherit; font-size: 13px;
  }
  input[type=search]:focus, select:focus {
    outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-sf);
  }
  select {
    background: var(--surface); border: 1px solid var(--line); color: var(--ink);
    padding: 8px 10px; border-radius: 7px; font: inherit; font-size: 13px;
  }
  .btn {
    background: var(--surface); border: 1px solid var(--line); color: var(--ink-2);
    padding: 8px 13px; border-radius: 7px; cursor: pointer; font: inherit; font-size: 13px;
    display: inline-flex; align-items: center; gap: 6px; text-decoration: none;
  }
  .btn:hover { border-color: var(--accent); color: var(--accent); background: var(--surface); }
  .btn.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
  .btn.primary:hover { filter: brightness(1.07); color: #fff; }
  .btn.danger:hover { border-color: var(--err); color: var(--err); }
  .toggle { display: inline-flex; align-items: center; gap: 7px; font-size: 12.5px; color: var(--ink-2); cursor: pointer; user-select: none; }
  .muted { color: var(--ink-3); font-size: 12.5px; font-variant-numeric: tabular-nums; }

  /* ---------- table ---------- */
  .tablecard {
    background: var(--surface); border: 1px solid var(--line);
    border-radius: var(--radius); box-shadow: var(--shadow); overflow: hidden;
  }
  /* overflow-x:auto also makes this a vertical scroll container, so the sticky
     header must be positioned against THIS box (top:0), not the page topbar. */
  .scroll { overflow: auto; max-height: calc(100vh - 250px); }
  table { border-collapse: separate; border-spacing: 0; width: 100%; font-size: 13px; }
  thead th {
    position: sticky; top: 0; z-index: 5; background: var(--surface-2);
    text-align: left; padding: 9px 14px; color: var(--ink-3); font-weight: 600;
    font-size: 10.8px; text-transform: uppercase; letter-spacing: .7px;
    box-shadow: inset 0 -1px 0 var(--line); white-space: nowrap;
  }
  tbody td { padding: 10px 14px; border-bottom: 1px solid var(--line-soft); vertical-align: middle; }
  tbody tr:last-child td { border-bottom: 0; }
  tbody tr { cursor: pointer; }
  tbody tr:hover td { background: var(--surface-2); }
  tbody tr.sel td { background: var(--accent-sf); }
  .mono { font-family: var(--mono); font-size: 12.3px; }
  .nowrap { white-space: nowrap; }
  .dim { color: var(--ink-3); }
  .clip { max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

  .chip {
    display: inline-block; font-size: 11px; font-weight: 600; letter-spacing: .3px;
    padding: 2px 8px; border-radius: 5px; background: var(--surface-3); color: var(--ink-2);
    font-family: var(--mono); white-space: nowrap;
  }
  .chip.GET    { background: var(--ok-sf);     color: var(--ok); }
  .chip.POST   { background: var(--accent-sf); color: var(--accent); }
  .chip.DELETE { background: var(--err-sf);    color: var(--err); }
  .chip.PUT, .chip.PATCH { background: var(--warn-sf); color: var(--warn); }
  .chip.ev { background: var(--accent-sf); color: var(--accent); }
  .chip.room { background: var(--surface-3); color: var(--ink-2); }
  .code { color: var(--ok); font-weight: 600; font-family: var(--mono); font-variant-numeric: tabular-nums; }
  .code.bad { color: var(--err); }
  .lat { font-family: var(--mono); color: var(--ink-3); font-variant-numeric: tabular-nums; }

  .empty { padding: 56px 20px; text-align: center; color: var(--ink-3); }
  .empty .big { font-size: 26px; opacity: .35; margin-bottom: 10px; }
  .empty code {
    background: var(--surface-2); padding: 2px 6px; border-radius: 4px;
    font-family: var(--mono); font-size: 12px; color: var(--ink-2);
  }

  /* ---------- API cards ---------- */
  .apicard {
    background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius);
    box-shadow: var(--shadow); margin-bottom: 12px; overflow: hidden;
  }
  .apicard .head {
    display: flex; align-items: center; gap: 11px; padding: 14px 18px; cursor: pointer;
  }
  .apicard .head:hover { background: var(--surface-2); }
  .apicard .route { font-family: var(--mono); font-size: 13.5px; font-weight: 600; }
  .apicard .sum { color: var(--ink-2); font-size: 13px; }
  .apicard .caret { margin-left: auto; color: var(--ink-3); transition: transform .16s; font-size: 11px; }
  .apicard.open .caret { transform: rotate(90deg); }
  .apicard .body { display: none; padding: 0 18px 18px; border-top: 1px solid var(--line-soft); }
  .apicard.open .body { display: block; }
  .apicard .desc { color: var(--ink-2); margin: 14px 0 0; max-width: 78ch; }
  .cols { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-top: 14px; }
  .label {
    font-size: 10.8px; text-transform: uppercase; letter-spacing: .7px;
    color: var(--ink-3); font-weight: 600; margin-bottom: 6px;
    display: flex; align-items: center; gap: 8px;
  }
  .label .copy {
    margin-left: auto; border: 0; background: none; color: var(--ink-3);
    font: inherit; font-size: 11px; cursor: pointer; text-transform: none; letter-spacing: 0;
  }
  .label .copy:hover { color: var(--accent); }
  pre {
    background: var(--surface-2); border: 1px solid var(--line-soft); border-radius: 7px;
    padding: 11px 13px; margin: 0; overflow-x: auto; font-size: 12.3px;
    font-family: var(--mono); white-space: pre-wrap; word-break: break-word;
    color: var(--ink-2); line-height: 1.5;
  }

  /* ---------- drawer ---------- */
  .scrim {
    position: fixed; inset: 0; background: rgba(12,20,30,.42); opacity: 0;
    pointer-events: none; transition: opacity .18s; z-index: 40;
  }
  .scrim.show { opacity: 1; pointer-events: auto; }
  .drawer {
    position: fixed; top: 0; right: 0; height: 100vh; width: min(620px, 94vw);
    background: var(--surface); border-left: 1px solid var(--line); z-index: 50;
    transform: translateX(100%); transition: transform .22s cubic-bezier(.4,0,.2,1);
    display: flex; flex-direction: column; box-shadow: -8px 0 30px rgba(12,20,30,.14);
  }
  .drawer.show { transform: none; }
  .drawer .dhead {
    display: flex; align-items: flex-start; gap: 12px; padding: 16px 20px;
    border-bottom: 1px solid var(--line);
  }
  .drawer .dhead h2 { margin: 0; font-size: 14.5px; font-weight: 650; }
  .drawer .dhead .s { font-size: 12px; color: var(--ink-3); margin-top: 3px; font-family: var(--mono); }
  .drawer .dbody { padding: 18px 20px 40px; overflow-y: auto; }
  .drawer .grid {
    display: grid; grid-template-columns: 132px 1fr; gap: 7px 14px;
    font-size: 13px; margin-bottom: 18px;
  }
  .drawer .grid dt { color: var(--ink-3); font-size: 12.5px; }
  .drawer .grid dd { margin: 0; font-family: var(--mono); font-size: 12.4px; word-break: break-word; }
  .drawer .block { margin-bottom: 16px; }

  @media (max-width: 1080px) { .stats { grid-template-columns: repeat(2, 1fr); } .cols { grid-template-columns: 1fr; } }
  @media (max-width: 860px) {
    .shell { grid-template-columns: 1fr; }
    aside { position: static; height: auto; flex-direction: row; align-items: center;
            overflow-x: auto; border-right: 0; border-bottom: 1px solid var(--line); }
    .brand { border: 0; padding: 12px 16px; }
    .brand .mark { display: none; }
    .nav { flex-direction: row; padding: 8px; }
    .nav .sec, .side-foot { display: none; }
    .scroll { max-height: none; }
    .content { padding: 16px; }
    input[type=search] { min-width: 0; width: 100%; }
  }
</style>
</head>
<body>
<div class="shell">

  <aside>
    <div class="brand">
      <div class="mark">DB</div>
      <div class="name">FastAPI DB API</div>
      <div class="role">Provider &middot; cluster3</div>
    </div>
    <div class="nav">
      <div class="sec">Console</div>
      <button class="active" data-tab="apis"><span class="ico">&#9783;</span> APIs <span class="badge" id="navApis">0</span></button>
      <button data-tab="logs"><span class="ico">&#9202;</span> Logs <span class="badge" id="navLogs">0</span></button>
      <button data-tab="inbox"><span class="ico">&#9993;</span> Inbox <span class="badge" id="navInbox">0</span></button>
    </div>
    <div class="side-foot">
      <div class="kv"><span>Version</span><span id="fVersion">2.0</span></div>
      <div class="kv"><span>Database</span><span id="fDb">-</span></div>
      <div class="kv"><span>Updated</span><span id="fUpdated">-</span></div>
    </div>
  </aside>

  <div class="main">
    <div class="topbar">
      <div>
        <h1 id="pageTitle">API Reference</h1>
        <div class="sub" id="pageSub">Endpoints exposed by this provider application</div>
      </div>
      <div class="grow"></div>
      <span class="status" id="dbStatus"><span class="dot"></span><span id="dbText">checking...</span></span>
      <button class="iconbtn" id="themeBtn" title="Toggle theme">&#9681;</button>
    </div>

    <div class="content">

      <!-- ===== APIs ===== -->
      <section class="panel show" id="panel-apis">
        <div class="toolbar">
          <div class="search">
            <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.7">
              <circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 L14 14"/></svg>
            <input type="search" id="apiFilter" placeholder="Search endpoints, paths, descriptions...">
          </div>
          <span class="muted" id="apiCount"></span>
          <div class="grow"></div>
          <button class="btn" onclick="setAllCards(true)">Expand all</button>
          <button class="btn" onclick="setAllCards(false)">Collapse all</button>
          <a class="btn primary" href="/docs" target="_blank" rel="noopener">OpenAPI &#8599;</a>
        </div>
        <div id="apiList"></div>
      </section>

      <!-- ===== Logs ===== -->
      <section class="panel" id="panel-logs">
        <div class="stats">
          <div class="stat"><div class="t">Total calls</div><div class="v" id="sTotal">0</div><div class="m" id="sWindow">in buffer</div></div>
          <div class="stat"><div class="t">Successful</div><div class="v ok" id="sOk">0</div><div class="m" id="sOkPct">-</div></div>
          <div class="stat"><div class="t">Failed</div><div class="v" id="sErr">0</div><div class="m" id="sErrPct">-</div></div>
          <div class="stat"><div class="t">Avg latency</div><div class="v" id="sLat">-</div><div class="m" id="sLast">no calls yet</div></div>
        </div>
        <div class="toolbar">
          <div class="search">
            <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.7">
              <circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 L14 14"/></svg>
            <input type="search" id="logFilter" placeholder="Search caller, endpoint, body, acknowledgement...">
          </div>
          <select id="logStatus">
            <option value="">All statuses</option>
            <option value="ok">2xx / 3xx only</option>
            <option value="err">4xx / 5xx only</option>
          </select>
          <select id="logMethod"><option value="">All methods</option></select>
          <span class="muted" id="logCount"></span>
          <div class="grow"></div>
          <label class="toggle"><input type="checkbox" id="logAuto" checked> Live</label>
          <button class="btn" onclick="loadLogs(true)">Refresh</button>
          <button class="btn danger" onclick="clearLogs()">Clear</button>
        </div>
        <div class="tablecard">
          <div class="scroll">
            <table>
              <thead><tr>
                <th>Time (UTC)</th><th>Caller</th><th>Method</th><th>Endpoint</th>
                <th>Status</th><th>Latency</th><th>Request</th><th>Acknowledgement</th><th>Sent to</th>
              </tr></thead>
              <tbody id="logBody"></tbody>
            </table>
          </div>
          <div id="logEmpty" class="empty">
            <div class="big">&#9202;</div>
            No API calls recorded yet. Traffic to <code>/qry</code>, <code>/hello</code>
            and <code>/incoming_message</code> appears here.
          </div>
        </div>
      </section>

      <!-- ===== Inbox ===== -->
      <section class="panel" id="panel-inbox">
        <div class="stats">
          <div class="stat"><div class="t">Messages</div><div class="v" id="mTotal">0</div><div class="m">stored in inbox.json</div></div>
          <div class="stat"><div class="t">Senders</div><div class="v" id="mSenders">0</div><div class="m" id="mTopSender">-</div></div>
          <div class="stat"><div class="t">Event types</div><div class="v" id="mTypes">0</div><div class="m" id="mTopType">-</div></div>
          <div class="stat"><div class="t">Last received</div><div class="v" id="mLastV">-</div><div class="m" id="mLast">no messages yet</div></div>
        </div>
        <div class="toolbar">
          <div class="search">
            <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.7">
              <circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 L14 14"/></svg>
            <input type="search" id="inboxFilter" placeholder="Search sender, eventType, room, payload...">
          </div>
          <select id="inboxType"><option value="">All event types</option></select>
          <select id="inboxRoom"><option value="">All rooms</option></select>
          <span class="muted" id="inboxCount"></span>
          <div class="grow"></div>
          <label class="toggle"><input type="checkbox" id="inboxAuto" checked> Live</label>
          <button class="btn" onclick="loadInbox(true)">Refresh</button>
        </div>
        <div class="tablecard">
          <div class="scroll">
            <table>
              <thead><tr>
                <th>Received (UTC)</th><th>Sender</th><th>Event type</th><th>Room</th>
                <th>Method</th><th>Source IP</th><th>Event payload</th><th>Acknowledged</th>
              </tr></thead>
              <tbody id="inboxBody"></tbody>
            </table>
          </div>
          <div id="inboxEmpty" class="empty">
            <div class="big">&#9993;</div>
            No events received yet. POST an envelope to <code>/incoming_message</code>.
          </div>
        </div>
      </section>

    </div>
  </div>
</div>

<div class="scrim" id="scrim" onclick="closeDrawer()"></div>
<div class="drawer" id="drawer" role="dialog" aria-modal="true">
  <div class="dhead">
    <div>
      <h2 id="drawerTitle">Detail</h2>
      <div class="s" id="drawerSub"></div>
    </div>
    <div class="grow"></div>
    <button class="iconbtn" onclick="closeDrawer()" title="Close">&#10005;</button>
  </div>
  <div class="dbody" id="drawerBody"></div>
</div>

<script>
const $ = (id) => document.getElementById(id);

/* ---------- helpers ---------- */
function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
function pretty(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string") return v;
  try { return JSON.stringify(v, null, 2); } catch (e) { return String(v); }
}
function oneLine(v, n) {
  if (v === null || v === undefined || v === "") return "—";
  const s = typeof v === "string" ? v : JSON.stringify(v);
  return s.length > (n || 70) ? s.slice(0, n || 70) + "…" : s;
}
function clock(ts) {
  if (!ts) return "—";
  return String(ts).replace("T", " ").replace(/\.\d+/, "").replace(/(\+00:00|Z)$/, "");
}
function ago(ts) {
  if (!ts) return "—";
  const d = (Date.now() - Date.parse(ts)) / 1000;
  if (!isFinite(d)) return "—";
  if (d < 60) return Math.max(0, Math.round(d)) + "s ago";
  if (d < 3600) return Math.round(d / 60) + "m ago";
  if (d < 86400) return Math.round(d / 3600) + "h ago";
  return Math.round(d / 86400) + "d ago";
}
function options(sel, values) {
  const keep = sel.value;
  const first = sel.options[0].outerHTML;
  sel.innerHTML = first + values.map(v => `<option value="${esc(v)}">${esc(v)}</option>`).join("");
  if (values.indexOf(keep) >= 0) sel.value = keep;
}
async function copyText(btn, text) {
  try { await navigator.clipboard.writeText(text); } catch (e) { return; }
  const old = btn.textContent;
  btn.textContent = "copied";
  setTimeout(() => { btn.textContent = old; }, 1200);
}
function copyBtn(text) {
  return `<button class="copy" onclick="event.stopPropagation();copyText(this, this.dataset.t)" data-t="${esc(text)}">copy</button>`;
}

/* ---------- theme ---------- */
const stored = (function () { try { return localStorage.getItem("provider-theme"); } catch (e) { return null; } })();
const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
setTheme(stored || (prefersDark ? "dark" : "light"));
function setTheme(t) {
  document.documentElement.setAttribute("data-theme", t);
  try { localStorage.setItem("provider-theme", t); } catch (e) {}
}
$("themeBtn").onclick = () =>
  setTheme(document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark");

/* ---------- navigation ---------- */
const TITLES = {
  apis:  ["API Reference", "Endpoints exposed by this provider application"],
  logs:  ["Request Log", "Every inbound call with its request body and the acknowledgement returned"],
  inbox: ["Event Inbox", "Events received on /incoming_message and persisted to inbox.json"]
};
document.querySelectorAll(".nav button").forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll(".nav button").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    const tab = btn.dataset.tab;
    ["apis", "logs", "inbox"].forEach(t => $("panel-" + t).classList.toggle("show", t === tab));
    $("pageTitle").textContent = TITLES[tab][0];
    $("pageSub").textContent = TITLES[tab][1];
    closeDrawer();
    if (tab === "logs") loadLogs();
    if (tab === "inbox") loadInbox();
  };
});

/* ---------- drawer ---------- */
function openDrawer(title, sub, html) {
  $("drawerTitle").innerHTML = title;
  $("drawerSub").textContent = sub;
  $("drawerBody").innerHTML = html;
  $("drawer").classList.add("show");
  $("scrim").classList.add("show");
}
function closeDrawer() {
  $("drawer").classList.remove("show");
  $("scrim").classList.remove("show");
  document.querySelectorAll("tbody tr.sel").forEach(r => r.classList.remove("sel"));
}
document.addEventListener("keydown", e => { if (e.key === "Escape") closeDrawer(); });
function selectRow(tr) {
  document.querySelectorAll("tbody tr.sel").forEach(r => r.classList.remove("sel"));
  tr.classList.add("sel");
}

/* ---------- APIs ---------- */
let apiData = [];
async function loadApis() {
  try {
    const r = await fetch("/api/endpoints");
    apiData = (await r.json()).endpoints || [];
    $("navApis").textContent = apiData.length;
    renderApis();
  } catch (e) {}
}
function renderApis() {
  const q = $("apiFilter").value.toLowerCase();
  const items = apiData.filter(a =>
    !q || (a.path + " " + a.method + " " + a.summary + " " + a.description).toLowerCase().includes(q));
  $("apiCount").textContent = items.length + " of " + apiData.length + " endpoints";
  $("apiList").innerHTML = items.map((a, i) => `
    <div class="apicard${i === 0 ? " open" : ""}">
      <div class="head" onclick="this.parentNode.classList.toggle('open')">
        <span class="chip ${esc(a.method)}">${esc(a.method)}</span>
        <span class="route">${esc(a.path)}</span>
        <span class="sum">${esc(a.summary)}</span>
        <span class="caret">&#9654;</span>
      </div>
      <div class="body">
        <p class="desc">${esc(a.description)}</p>
        <div class="cols">
          <div>
            <div class="label">Request body ${a.request ? copyBtn(JSON.stringify(a.request, null, 2)) : ""}</div>
            <pre>${esc(a.request ? pretty(a.request) : "No request body")}</pre>
          </div>
          <div>
            <div class="label">Example response ${copyBtn(JSON.stringify(a.response, null, 2))}</div>
            <pre>${esc(pretty(a.response))}</pre>
          </div>
        </div>
      </div>
    </div>`).join("") || `<div class="tablecard"><div class="empty">No endpoint matches “${esc(q)}”.</div></div>`;
}
function setAllCards(open) {
  document.querySelectorAll(".apicard").forEach(c => c.classList.toggle("open", open));
}
$("apiFilter").oninput = renderApis;

/* ---------- Logs ---------- */
let logData = [];
async function loadLogs(force) {
  try {
    const r = await fetch("/api/logs?limit=200");
    const j = await r.json();
    logData = j.logs || [];
    $("navLogs").textContent = j.total != null ? j.total : logData.length;
    options($("logMethod"), [...new Set(logData.map(l => l.method))].sort());
    renderLogs();
  } catch (e) {}
}
function logFiltered() {
  const q = $("logFilter").value.toLowerCase();
  const st = $("logStatus").value;
  const me = $("logMethod").value;
  return logData.filter(l => {
    if (me && l.method !== me) return false;
    if (st === "ok" && l.status_code >= 400) return false;
    if (st === "err" && l.status_code < 400) return false;
    return !q || JSON.stringify(l).toLowerCase().includes(q);
  });
}
function renderLogs() {
  const items = logFiltered();
  const ok = logData.filter(l => l.status_code < 400).length;
  const err = logData.length - ok;
  const pct = (n) => logData.length ? Math.round(n * 100 / logData.length) + "% of window" : "—";
  $("sTotal").textContent = logData.length;
  $("sWindow").textContent = "most recent " + logData.length + " in buffer";
  $("sOk").textContent = ok;  $("sOkPct").textContent = pct(ok);
  $("sErr").textContent = err; $("sErrPct").textContent = pct(err);
  $("sErr").className = "v" + (err ? " err" : "");
  const lat = logData.map(l => l.duration_ms).filter(n => typeof n === "number");
  $("sLat").textContent = lat.length ? (lat.reduce((a, b) => a + b, 0) / lat.length).toFixed(1) + " ms" : "—";
  $("sLast").textContent = logData.length ? "last call " + ago(logData[0].timestamp) : "no calls yet";
  $("logCount").textContent = items.length + " of " + logData.length + " shown";
  $("logEmpty").style.display = items.length ? "none" : "";

  $("logBody").innerHTML = items.map((l, i) => `
    <tr onclick="showLog(${i}, this)">
      <td class="mono nowrap">${esc(clock(l.timestamp))}</td>
      <td class="mono nowrap">${esc(l.caller)}</td>
      <td><span class="chip ${esc(l.method)}">${esc(l.method)}</span></td>
      <td class="mono">${esc(l.endpoint)}</td>
      <td><span class="code${l.status_code >= 400 ? " bad" : ""}">${esc(l.status_code)}</span></td>
      <td class="lat nowrap">${esc(l.duration_ms)} ms</td>
      <td class="clip mono dim">${esc(oneLine(l.request_body))}</td>
      <td class="clip mono dim">${esc(oneLine(l.acknowledgement))}</td>
      <td class="mono nowrap dim">${esc(l.responded_to)}</td>
    </tr>`).join("");
  window._logView = items;
}
function showLog(i, tr) {
  const l = window._logView[i];
  selectRow(tr);
  openDrawer(
    `<span class="chip ${esc(l.method)}">${esc(l.method)}</span> <span class="mono">${esc(l.endpoint)}</span>`,
    clock(l.timestamp) + " UTC · " + ago(l.timestamp),
    `<dl class="grid">
      <dt>Caller</dt><dd>${esc(l.caller)}${l.caller_port ? ":" + esc(l.caller_port) : ""}</dd>
      <dt>Status</dt><dd><span class="code${l.status_code >= 400 ? " bad" : ""}">${esc(l.status_code)}</span></dd>
      <dt>Latency</dt><dd>${esc(l.duration_ms)} ms</dd>
      <dt>Content type</dt><dd>${esc(l.content_type || "—")}</dd>
      <dt>User agent</dt><dd>${esc(l.user_agent || "—")}</dd>
      <dt>Query params</dt><dd>${esc(oneLine(l.query_params, 200))}</dd>
      <dt>Acknowledged to</dt><dd>${esc(l.responded_to)}</dd>
      <dt>Trace id</dt><dd>${esc(l.id)}</dd>
    </dl>
    <div class="block">
      <div class="label">Request body ${copyBtn(pretty(l.request_body))}</div>
      <pre>${esc(pretty(l.request_body))}</pre>
    </div>
    <div class="block">
      <div class="label">Acknowledgement returned ${copyBtn(pretty(l.acknowledgement))}</div>
      <pre>${esc(pretty(l.acknowledgement))}</pre>
    </div>`
  );
}
["logFilter", "logStatus", "logMethod"].forEach(id => {
  $(id).oninput = renderLogs; $(id).onchange = renderLogs;
});
async function clearLogs() {
  if (!confirm("Clear the request log? Entries are removed from memory and api_logs.json.")) return;
  await fetch("/api/logs", { method: "DELETE" });
  closeDrawer();
  loadLogs(true);
}

/* ---------- Inbox ---------- */
let inboxData = [];
async function loadInbox(force) {
  try {
    const r = await fetch("/api/inbox?limit=200");
    const j = await r.json();
    inboxData = j.messages || [];
    $("navInbox").textContent = j.total != null ? j.total : inboxData.length;
    options($("inboxType"), [...new Set(inboxData.map(evType))].sort());
    options($("inboxRoom"), [...new Set(inboxData.map(m => m.room || "—"))].sort());
    renderInbox();
  } catch (e) {}
}
function evType(m) { return m.event_type || m.request_type || "message"; }
function evPayload(m) { return m.event !== undefined && m.event !== null ? m.event : m.message; }
function renderInbox() {
  const q = $("inboxFilter").value.toLowerCase();
  const ty = $("inboxType").value, rm = $("inboxRoom").value;
  const items = inboxData.filter(m => {
    if (ty && evType(m) !== ty) return false;
    if (rm && (m.room || "—") !== rm) return false;
    return !q || JSON.stringify(m).toLowerCase().includes(q);
  });
  const senders = new Set(inboxData.map(m => m.sender));
  const types = new Set(inboxData.map(evType));
  $("mTotal").textContent = inboxData.length;
  $("mSenders").textContent = senders.size;
  $("mTopSender").textContent = inboxData.length ? "latest: " + inboxData[0].sender : "—";
  $("mTypes").textContent = types.size;
  $("mTopType").textContent = inboxData.length ? "latest: " + evType(inboxData[0]) : "—";
  $("mLastV").textContent = inboxData.length ? ago(inboxData[0].received_at) : "—";
  $("mLast").textContent = inboxData.length ? clock(inboxData[0].received_at) + " UTC" : "no messages yet";
  $("inboxCount").textContent = items.length + " of " + inboxData.length + " shown";
  $("inboxEmpty").style.display = items.length ? "none" : "";

  $("inboxBody").innerHTML = items.map((m, i) => `
    <tr onclick="showMsg(${i}, this)">
      <td class="mono nowrap">${esc(clock(m.received_at))}</td>
      <td class="nowrap">${esc(m.sender)}</td>
      <td><span class="chip ev">${esc(evType(m))}</span></td>
      <td><span class="chip room">${esc(m.room || "—")}</span></td>
      <td><span class="chip ${esc(m.http_method)}">${esc(m.http_method)}</span></td>
      <td class="mono nowrap dim">${esc(m.source_ip)}</td>
      <td class="clip mono dim">${esc(oneLine(evPayload(m)))}</td>
      <td class="clip dim">${esc(oneLine(m.acknowledgement, 44))}</td>
    </tr>`).join("");
  window._msgView = items;
}
function showMsg(i, tr) {
  const m = window._msgView[i];
  selectRow(tr);
  openDrawer(
    `<span class="chip ev">${esc(evType(m))}</span> <span class="mono">${esc(m.message_id)}</span>`,
    clock(m.received_at) + " UTC · " + ago(m.received_at),
    `<dl class="grid">
      <dt>Sender</dt><dd>${esc(m.sender)}</dd>
      <dt>Event type</dt><dd>${esc(evType(m))}</dd>
      <dt>Room</dt><dd>${esc(m.room || "—")}</dd>
      <dt>Source IP</dt><dd>${esc(m.source_ip)}</dd>
      <dt>Method</dt><dd>${esc(m.http_method)} ${esc(m.endpoint)}</dd>
      <dt>Content type</dt><dd>${esc(m.content_type || "—")}</dd>
      <dt>User agent</dt><dd>${esc(m.user_agent || "—")}</dd>
      <dt>Acknowledged to</dt><dd>${esc(m.acknowledged_to)}</dd>
    </dl>
    <div class="block">
      <div class="label">Event payload ${copyBtn(pretty(evPayload(m)))}</div>
      <pre>${esc(pretty(evPayload(m)))}</pre>
    </div>
    <div class="block">
      <div class="label">Full envelope received ${copyBtn(pretty(m.message))}</div>
      <pre>${esc(pretty(m.message))}</pre>
    </div>
    <div class="block">
      <div class="label">Acknowledgement returned</div>
      <pre>${esc(m.acknowledgement)}</pre>
    </div>`
  );
}
["inboxFilter", "inboxType", "inboxRoom"].forEach(id => {
  $(id).oninput = renderInbox; $(id).onchange = renderInbox;
});

/* ---------- health ---------- */
async function loadHealth() {
  const el = $("dbStatus");
  try {
    const h = await (await fetch("/health")).json();
    const ok = h.database === "connected";
    el.className = "status " + (ok ? "ok" : "err");
    $("dbText").textContent = ok ? "Database connected" : "Database unavailable";
    $("fDb").textContent = ok ? "connected" : "error";
    $("fDb").title = h.database;
    $("fUpdated").textContent = clock(h.timestamp).split(" ")[1] || "—";
  } catch (e) {
    el.className = "status err";
    $("dbText").textContent = "Application unreachable";
    $("fDb").textContent = "unreachable";
  }
}

loadApis(); loadHealth(); loadLogs(); loadInbox();
setInterval(loadHealth, 15000);
setInterval(() => { if ($("logAuto").checked) loadLogs(); }, 5000);
setInterval(() => { if ($("inboxAuto").checked) loadInbox(); }, 5000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
@app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)
