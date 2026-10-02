# Payanam 2.0

Stochastic, multi-modal, state-wide transit routing engine for Tamil Nadu.

CP-SAT orienteering on a Ray cluster, orchestrated with Temporal Saga
compensations, grounded in Memgraph, driven by live Kafka signals, and backed
by a Redis geospatial fleet index for real-time cab dispatch.

## Architecture

| Layer | Technology | Responsibility |
|---|---|---|
| Solver | OR-Tools CP-SAT | Time-dependent stochastic orienteering with **disconnected service windows** |
| Compute | Ray Core | Heavy solving dispatched off the API event loop (`@ray.remote`) |
| Orchestration | Temporal.io | Long-running `ItinerarySaga` with Saga compensations and reactive signals |
| Graph | Memgraph (Bolt) | Tamil Nadu transit topology, idempotently seeded |
| Streaming | Redpanda / Kafka | Live traffic disruptions forwarded as workflow signals |
| Fleet | Redis GEO | Driver index + atomic cab locking for fallback dispatch |

### Reactive routing

A running workflow is long-lived, so a plan made at T+0 can be invalidated at
T+30s. `ItineraryWorkflow` accepts `transit_update` signals carrying a degraded
confirmation probability; when a still-pending leg falls below
`DEGRADED_THRESHOLD` (0.15) the workflow unwinds its Saga and books a
substitute â€” the same compensation path a booking failure takes, entered from a
different trigger.

Signals never mutate history: the handler only queues the update, and the main
coroutine drains it at deterministic safe points.

### Disconnected windows

South Indian temples close for the afternoon. Each `(node, window)` pair gets a
boolean literal, `OnlyEnforceIf`-guarded interval constraints, and a final
`model.AddBoolOr([...])` requiring at least one session to be satisfied.
The demo circuit splits across both sessions rather than forcing a midday visit.

### Fleet matching

Two Temporal workflows routinely reach the cab fallback simultaneously, so the
claim is an optimistic transaction: `WATCH` the driver, verify status, `MULTI`
the status flip + GEO-set move + `SET NX EX` lease, `EXEC`. A lost race is not
an error â€” the caller simply tries the next-nearest cab.

## Layout

```
payanam/
â”œâ”€â”€ app/
â”‚   â”œâ”€â”€ solver/cp_router.py      # CP-SAT model, @ray.remote entrypoint
â”‚   â”œâ”€â”€ workflows/
â”‚   â”‚   â”œâ”€â”€ itinerary.py         # Saga + transit_update signal + live query
â”‚   â”‚   â””â”€â”€ activities.py        # book_train / cancel_train / book_cab / book_bus
â”‚   â”œâ”€â”€ graph/
â”‚   â”‚   â”œâ”€â”€ seed_tn.py           # Tamil Nadu topology + hub coordinates
â”‚   â”‚   â”œâ”€â”€ repository.py        # Memgraph | in-memory backend
â”‚   â”‚   â””â”€â”€ state.py             # Cypher + subgraph routing-candidate queries
â”‚   â”œâ”€â”€ fleet/
â”‚   â”‚   â”œâ”€â”€ state.py             # Redis GEO index, GEOADD / GEOSEARCH
â”‚   â”‚   â””â”€â”€ matcher.py           # bipartite matching + atomic driver locking
â”‚   â”œâ”€â”€ ingestion/stream.py      # Kafka -> Temporal signal bridge
â”‚   â””â”€â”€ api/                     # routing + driver telemetry endpoints
â””â”€â”€ tests/                       # solver, saga, e2e, fleet matching
```

## Quick start

```bash
docker compose up -d          # memgraph, redpanda, temporal, redis, ray
pip install -r requirements.txt
python -m app.graph.seed_tn   # idempotent Tamil Nadu seed
python -m app.worker &
uvicorn app.main:app --reload
```

Interactive API docs: <http://localhost:8000/docs>
Temporal UI: <http://localhost:8233>

---

## Running the full stack in isolation

Host ports are a **global resource**. If another project already owns 3000,
7687, 9092, 6379 or any other port, `docker compose up` fails with
`Bind for 0.0.0.0:3000 failed: port is already allocated`.

Every published port is parameterised with a default, so the fix is to copy
`.env.example` to `.env` and shift **only the host side**:

```bash
cp .env.example .env
```

For a machine where another stack owns the defaults, use the isolated profile
already documented in `.env.example`:

```dotenv
MEMGRAPH_BOLT_HOST_PORT=7690
MEMGRAPH_LAB_HOST_PORT=3001
MEMGRAPH_METRICS_HOST_PORT=9091
REDIS_HOST_PORT=6389
REDPANDA_KAFKA_HOST_PORT=9095
REDPANDA_ADMIN_HOST_PORT=9645
POSTGRES_HOST_PORT=5433
TEMPORAL_GRPC_HOST_PORT=7234
TEMPORAL_UI_HOST_PORT=8234
API_HOST_PORT=8001
RAY_GCS_HOST_PORT=6381
RAY_DASHBOARD_HOST_PORT=8266
RAY_CLIENT_HOST_PORT=10002
```

Then:

```bash
docker compose up -d
docker compose ps          # wait until every service reports (healthy)
```

### You do NOT change any application setting

Only the **host** side of a mapping moves; the **container** port is fixed and
services inside the compose network keep using canonical ports and internal
hostnames:

| Purpose | In-network address (never changes) |
|---|---|
| Memgraph Bolt | `bolt://memgraph:7687` |
| Memgraph Lab | `http://memgraph:3000` |
| Redis | `redis://redis:6379/0` |
| Redpanda | `redpanda:9092` |
| Temporal | `temporal:7233` |
| Ray GCS | `ray://ray-head:6379` |

`docker-compose.yml` sets these, and every one is overridable by a standard
environment variable â€” `MEMGRAPH_URI`, `REDIS_URL`, `KAFKA_BOOTSTRAP_SERVERS`,
`TEMPORAL_HOST`, `RAY_ADDRESS`.

### From the host, use your shifted ports

```bash
curl -s localhost:8001/health    # API
open http://localhost:3001       # Memgraph Lab
open http://localhost:8234       # Temporal UI
open http://localhost:8266       # Ray dashboard
```

Running the app **outside** Docker against the shifted stack:

```bash
export MEMGRAPH_URI=bolt://localhost:7690
export REDIS_URL=redis://localhost:6389/0
export TEMPORAL_HOST=localhost:7234
uvicorn app.main:app --reload
```

### Startup resilience

The API and worker usually boot *before* Memgraph, Temporal and Redis finish
initialising. Rather than crash-loop, `app/startup.py` retries the
connection-establishment step, bounded by `STARTUP_MAX_ATTEMPTS` /
`STARTUP_BACKOFF_SECONDS` (defaults 30 Ã— 2 s).

Retrying is deliberately narrow â€” only transport failures (`ConnectionError`,
`OSError`, `ServiceUnavailable`, â€¦). A `ClientError` from bad Cypher or an
`AttributeError` from a missing method is a **defect** and propagates on the
first attempt instead of being masked by retries. This mirrors the fail-closed
policy the repository layer uses.

On exhaustion the API logs loudly and reports `degraded` from `/health` rather
than crash-looping.

---

### Send a live disruption

```bash
rpk topic produce traffic_updates -b localhost:9092 <<EOF
{"workflow_id":"payanam-<id>","leg_id":"tn_vaigai_mas_mdu","p_confirm":0.0,"delay_minutes":45}
EOF
```

The workflow picks it up, unwinds, and dispatches a cab.

### Report a driver position

```bash
curl -X POST localhost:8000/api/v1/driver/location \
  -H 'content-type: application/json' \
  -d '{"driver_id":"drv-1","lon":80.2707,"lat":13.0830,"status":"AVAILABLE"}'
```

## Tests

```bash
pytest tests/ -q                        # full suite
python -m pytest tests/test_fleet_matching.py -v
PAYANAM_SANDBOX=1 python -m pytest tests/test_e2e_integration.py -v
```

The suite runs with no containers: Memgraph is represented by the in-memory
repository backend, Redis by `fakeredis`, and Temporal by its dev server.

## Configuration

Every setting defaults to `localhost` and is overridable by environment
variable (see `app/config.py`). The docker-compose stack points them at the
in-network hostnames.
