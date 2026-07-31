# Test App 4 (cluster3) — Documentation

Provider application for the Information Mediator: a single-file FastAPI service
that exposes a SQL query API, receives events from consumers, and ships with a
built-in web console showing its own API catalogue, request log and event inbox.

| Document | Audience | Contents |
|---|---|---|
| [architecture.md](architecture.md) | Architects, reviewers | Components, request flow, storage model, deployment topology, design decisions and limitations |
| [developer.md](developer.md) | Developers | Code layout, local setup, how each part works, how to extend it, build and release |
| [user.md](user.md) | Operators, consumers | Using the console, calling the APIs, sending events, reading the logs |
| [tester.md](tester.md) | QA | Test matrix, executable cases with expected results, edge cases, regression checklist |

## At a glance

| | |
|---|---|
| Application | `Simple FastAPI Database API` v2.0 |
| Source | [`app.py`](../app.py) — one file, no other Python modules |
| Runtime | Python 3.12, FastAPI + uvicorn, PyMySQL |
| Port | 8000 |
| Console | `/` and `/ui` |
| Image | `msahique/helloworld_test_k8s_app:v2` |
| Manifests | [`test_app.yaml`](../test_app.yaml) |
| State | `inbox.json`, `api_logs.json` under `IM_DATA_DIR` |

## Known issues

Both are open as of 2026-07-29 and are described in detail in the documents below.

1. **Console shows no data when served under a reverse-proxy path prefix.**
   The page loads at e.g. `http://host:9091/app1/`, but its JavaScript requests
   `/api/logs` instead of `/app1/api/logs`, so every data call 404s and all three
   tabs render empty. Direct access and `kubectl port-forward` are unaffected.
   See [user.md](user.md#the-console-loads-but-every-tab-is-empty) and
   [architecture.md](architecture.md#known-issues-and-limitations).
2. **`test_app.yaml` has a malformed `image:` key.** Line 58 currently reads
   `imagemsahique/helloworld_test_k8s_app:v2:` — the colon and space after
   `image` were lost, so `kubectl apply` rejects the Deployment. See
   [developer.md](developer.md#deploying-to-kubernetes).
