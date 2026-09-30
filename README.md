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
substitute — the same compensation path a booking failure takes, entered from a
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
an error — the caller simply tries the next-nearest cab.

## Layout

```
payanam/
├── app/
│   ├── solver/cp_router.py      # CP-SAT model, @ray.remote entrypoint
│   ├── workflows/
│   │   ├── itinerary.py         # Saga + transit_update signal + live query
│   │   └── activities.py        # book_train / cancel_train / book_cab / book_bus
│   ├── graph/
│   │   ├── seed_tn.py           # Tamil Nadu topology + hub coordinates
│   │   ├── repository.py        # Memgraph | in-memory backend
│   │   └── state.py             # Cypher + subgraph routing-candidate queries
│   ├── fleet/
│   │   ├── state.py             # Redis GEO index, GEOADD / GEOSEARCH
│   │   └── matcher.py           # bipartite matching + atomic driver locking
│   ├── ingestion/stream.py      # Kafka -> Temporal signal bridge
│   └── api/                     # routing + driver telemetry endpoints
└── tests/                       # solver, saga, e2e, fleet matching
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
