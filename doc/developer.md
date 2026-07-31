# Developer Guide

Everything is in one module: [`app.py`](../app.py). There are no other Python
files, no templates directory and no static assets — the container image is built
from `COPY app.py .` alone, and the web console lives in a string constant inside
that file.

---

## 1. Local setup

```powershell
cd c:\Users\Sahique\Desktop\new_workspace\2026\IM_new\Test_app_4_cluster3

# the workspace venv already has fastapi, uvicorn and pymysql
..\.venv\Scripts\python.exe -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

Open http://127.0.0.1:8000/.

Defaults point at a local database (`IM_DB_HOST=127.0.0.1`, `IM_DB_NAME=ca_db`).
Without one, the header shows *Database unavailable* and `/qry` returns 500 —
every other feature still works. To point elsewhere for a session:

```powershell
$env:IM_DB_HOST = "mysql.mycluster.svc"; $env:IM_DB_NAME = "test"
```

With `IM_DATA_DIR` unset, `inbox.json` and `api_logs.json` are written next to
`app.py`. Keep them out of source control.

---

## 2. Code map

Read `app.py` top to bottom; it is ordered as a pipeline.

| Section | What it does |
|---|---|
| Config block | `IM_DB_*`, `IM_DATA_DIR`, ring sizes, `IM_BODY_MAX`, `IM_LOG_HEALTH`, `IM_DEFAULT_ROOM` |
| `get_connection()` | One PyMySQL connection per request, `DictCursor`, `autocommit=True` |
| `jsonable()` | Recursively converts `datetime`, `date`, `time`, `Decimal`, `bytes`, `set` into JSON-safe values |
| `load_store()` / `save_store()` | Ring buffer ↔ JSON file, atomic via `.tmp` + `os.replace`, never fatal |
| `decode_body()` | Bytes → parsed JSON, or truncated text |
| `caller_of()` | `X-Forwarded-For` first hop, else peer IP, else `"unknown"` |
| `audit_middleware()` | Captures every non-console request/response pair |
| `API_CATALOG` | Data that drives the APIs tab |
| Business endpoints | `/health`, `/hello`, `/qry`, `/incoming_message` |
| Console API | `/api/endpoints`, `/api/logs`, `/api/inbox` |
| `DASHBOARD_HTML` | The whole front end, as an `r"""…"""` literal |
| `dashboard()` | Serves that string at `/` and `/ui` |

### Three things that will bite you if you change them

**1. The response must be drained and rebuilt.** In `audit_middleware`:

```python
chunks = [chunk async for chunk in response.body_iterator]
raw_response = b"".join(chunks)
...
return Response(content=raw_response, status_code=response.status_code,
                headers=dict(response.headers), media_type=response.media_type)
```

A streaming body can only be consumed once. If you log it and return the original
`response`, the client receives an empty body.

**2. Reading the request body in middleware depends on Starlette ≥ 0.28.** That
version introduced `_CachedRequest`, which replays the cached body to the
downstream endpoint. On older Starlette, `await request.body()` in a
`BaseHTTPMiddleware` consumes the stream and the endpoint hangs.
`requirements.txt` is unpinned, so `pip install` gets a recent version — verify
with `python -c "import starlette; print(starlette.__version__)"` if requests
ever start hanging after a rebuild.

**3. `DASHBOARD_HTML` is a raw string (`r"""`).** Backslashes are literal, so JS
regexes like `/(\+00:00|Z)$/` can be written normally. Consequences: the HTML must
never contain `"""`, and must not end with a backslash.

---

## 3. Working on the front end

The page is inline CSS + vanilla JS with no build step and no external requests
(a CDN would fail in an air-gapped cluster). Structure:

- `BASE`-less `fetch()` calls to `/api/endpoints`, `/api/logs`, `/api/inbox`, `/health`
- `loadX()` fetches → `renderX()` filters and paints → `showX()` opens the drawer
- Theme is `data-theme` on `<html>`, persisted in `localStorage`, defaulting to
  `prefers-color-scheme`
- Polling: health every 15s, logs and inbox every 5s while their *Live* toggle is on

**Always escape interpolated values with `esc()`.** Rows are built with template
strings; log entries contain attacker-controlled text (request bodies, user
agents). `esc()` covers `& < > " '`.

Editing a 700-line string literal in place is painful. The practical loop:

1. Extract the current page to a scratch file:
   `python -c "import app,re;open('dash.html','w',encoding='utf-8').write(app.DASHBOARD_HTML)"`
2. Edit `dash.html` with normal HTML tooling.
3. Splice it back — replace the `DASHBOARD_HTML = r"""…"""` literal with the file
   contents, asserting the file contains no `"""` and does not end in a backslash.
4. Verify: `node --check` the extracted `<script>` block, and confirm the CSS
   braces balance.

### CSS gotcha already fixed — do not reintroduce

`.scroll` uses `overflow: auto`. Setting only `overflow-x: auto` still makes the
element a *vertical* scroll container, which means `position: sticky; top: 60px`
on `thead th` pins the header 60px inside the card — visually landing in the
middle of the rows. The header is positioned against `.scroll` (`top: 0`), and its
bottom rule is an `inset` box-shadow because a real `border-bottom` on a sticky
cell can detach under `border-collapse: separate`.

---

## 4. Adding an endpoint

1. Write the handler. Sync `def` runs in the threadpool (right for PyMySQL);
   `async def` for anything that awaits.
2. Append an entry to `API_CATALOG` with `method`, `path`, `summary`,
   `description`, `request`, `response` — the APIs tab and the endpoint count
   update automatically.
3. Decide whether it should be audited. Console/read-model routes belong in
   `NO_LOG_PATHS`; business endpoints do not.
4. Pass any DB rows through `jsonable()` before returning them.

Extending the event parser: read new fields inside `incoming_message()`, keep the
existing `or`-chain fallbacks, and keep writing `request_type` alongside
`event_type` so inbox entries written by older versions still render.

---

## 5. Testing changes

There is no test suite in the repo. Use `fastapi.testclient.TestClient` with a
fake connection object so `/qry` can be exercised without MySQL — a stub class
exposing `cursor()`, `execute()`, `description`, `fetchall()`, `rowcount`,
`lastrowid` and `close()` is enough, assigned over `app.get_connection`. Set
`IM_DATA_DIR` to a temp directory *before* importing the module, or the test run
will write into the source folder.

[tester.md](tester.md) has the full case list and expected results.

---

## 6. Build and release

```powershell
docker build -t msahique/helloworld_test_k8s_app:v2 .
docker run --rm -p 8000:8000 -e IM_DB_HOST=host.docker.internal msahique/helloworld_test_k8s_app:v2
docker push msahique/helloworld_test_k8s_app:v2
```

**Bump the tag on every release.** The Deployment uses
`imagePullPolicy: IfNotPresent`; rebuilding under an existing tag leaves the node
running the cached old image. For local clusters use
`kind load docker-image …` or `minikube image load …` instead of pushing.

The Dockerfile sets `ENV IM_DATA_DIR=/app/data` and creates the directory, so the
JSON files land on the mounted volume rather than the container's writable layer.

---

## 7. Deploying to Kubernetes

```powershell
kubectl apply -f test_app.yaml
kubectl rollout status deploy/fastapi-db-api
kubectl port-forward svc/fastapi-db-api 8000:8000
```

> **`test_app.yaml` will not apply as it stands.** Line 58 reads
> `imagemsahique/helloworld_test_k8s_app:v2:` — the `image` key lost its colon and
> space. It must be:
>
> ```yaml
>         image: msahique/helloworld_test_k8s_app:v2
> ```

Resources created: ConfigMap `fastapi-db-config`, PVC `fastapi-db-api-data`
(1Gi RWO), Deployment `fastapi-db-api` (1 replica, `Recreate`), Service
`fastapi-db-api` (ClusterIP 8000). Keep `replicas: 1` — see
[architecture.md](architecture.md#5-deployment-topology).

---

## 8. Open work

**The console shows no data behind a reverse-proxy prefix.** Served at
`http://host:9091/app1/`, the page loads but its JS calls
`http://host:9091/api/logs`, which the proxy does not route → 404 on every data
call, all tabs empty. The fix, not yet applied:

```js
// derive the mount prefix from the URL the page was actually loaded from
const BASE = location.pathname.replace(/\/+$/, "").replace(/\/ui$/, "");
const api = (path) => BASE + path;
// then: fetch(api("/api/logs?limit=200")), fetch(api("/health")), …
```

Plus `root_path=os.getenv("IM_ROOT_PATH", "")` on the `FastAPI()` constructor so
`/docs` and `/openapi.json` advertise prefixed URLs. `root_path` only affects URL
generation — it assumes the proxy strips the prefix before forwarding, which is
what the current deployment does (the page is reachable at `/app1/`, so the pod is
receiving `/`).
