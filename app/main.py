"""FastAPI application for Project payanam.

Startup order (all best-effort -- the API stays up if a dependency is down):

1. connect to Temporal and ensure the namespace exists,
2. initialise Ray (cluster address, or an embedded local runtime),
3. connect to Memgraph and seed the demo state on an empty graph,
4. start the Redpanda traffic-update consumer.

Shutdown reverses it: consumer stopped, Ray shut down, clients closed.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.driver_routes import router as driver_router
from .api.routes import router as api_router
from .config import get_settings
from .graph.state import AsyncMemgraphClient
from .ingestion.stream import TrafficUpdateConsumer
from .temporal_client import (
    connect_temporal,
    ensure_namespace,
)
from .startup import retry_async
from .workflows.activities import describe_activities

settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
log = logging.getLogger("payanam")


# --------------------------------------------------------------------------- #
# Lifespan
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    log.info("starting %s (env=%s)", settings.app_name, settings.app_env)

    # ---------------------------------------------------------- temporal
    # Wait for the Temporal frontend: without it no workflow can start, and
    # exiting here would just produce a Docker restart loop.
    async def _temporal() -> Any:
        client = await connect_temporal(settings)
        await ensure_namespace(client, settings.temporal_namespace)
        return client

    app.state.temporal = await retry_async("temporal", _temporal, settings=settings)

    # --------------------------------------------------------------- ray
    app.state.ray_ready = False  # Phase 6: no Ray Client in this process

    # ---------------------------------------------------------- memgraph
    memgraph = AsyncMemgraphClient(settings)
    app.state.memgraph = memgraph
    connected = await retry_async(
        "memgraph",
        memgraph.connect,
        settings=settings,
        required=False,  # /health still reports degraded; don't hard-fail
    )
    if connected:
        try:
            seeded = await memgraph.seed_demo_state()
            log.info("memgraph seeded: %s", seeded)
        except Exception as exc:  # noqa: BLE001
            log.warning("memgraph seed skipped: %s", exc)

    # ------------------------------------------------------------- redis
    # The fleet index must be reachable before we accept dispatch requests.
    from .fleet.state import get_fleet_state, probe_fleet_state

    app.state.fleet_ok = await retry_async(
        "redis",
        probe_fleet_state,
        settings=settings,
        required=False,
    )

    # ---------------------------------------------- phase 2: Tamil Nadu seed
    # Idempotent: safe on every boot. The repository layer fails closed, so a
    # reachable-but-broken graph surfaces here rather than being papered over.
    try:
        from .graph.repository import get_repository
        from .graph.seed_tn import seed_tamil_nadu

        app.state.graph = get_repository()
        counts = seed_tamil_nadu(app.state.graph)
        log.info("tamil nadu topology seeded via %s", counts)
    except Exception as exc:  # noqa: BLE001 - never block startup
        log.warning("Tamil Nadu seeding skipped: %s", exc)

    # ------------------------------------------------------------ kafka
    async def sink(edge_id: str, multiplier: float) -> None:
        await memgraph.apply_edge_multiplier(edge_id, multiplier)

    consumer = TrafficUpdateConsumer(settings, on_update=sink)
    app.state.consumer = consumer
    consumer.start()

    try:
        yield
    finally:
        log.info("shutting down %s", settings.app_name)
        await consumer.stop()
        await memgraph.close()


# --------------------------------------------------------------------------- #
# Application
# --------------------------------------------------------------------------- #
app = FastAPI(
    title="Project payanam",
    description=(
        "Stochastic, multi-modal, state-wide transit routing engine. "
        "CP-SAT orienteering on Ray, orchestrated with Temporal Sagas, "
        "grounded in Memgraph and live-fed by Redpanda."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
app.include_router(driver_router)


@app.get("/health", tags=["ops"], summary="Liveness and dependency status")
async def health() -> dict:
    """Reports each dependency so orchestrators can gate traffic."""
    temporal_ok = app.state.temporal is not None
    memgraph_ok = False
    memgraph = getattr(app.state, "memgraph", None)
    if memgraph is not None:
        try:
            memgraph_ok = bool(await memgraph.run("RETURN 1 AS ok"))
        except Exception:  # noqa: BLE001
            memgraph_ok = False

    # ---------------------------------------------------------------- fleet
    fleet_info: Dict[str, Any] = {"configured": False}
    try:
        from .fleet.state import get_fleet_state

        fleet = get_fleet_state()
        fleet_info = {
            "configured": True,
            "available": await fleet.available_count(),
            "locked": await fleet.locked_count(),
        }
    except Exception as exc:  # noqa: BLE001 - never fail health on fleet
        fleet_info = {"configured": False, "error": str(exc)}

    # Phase 6: health now asks the Ray *dashboard* over REST. No Ray Client,
    # no remote task, no import of ray in the API process.
    from .solver.ray_dispatch import dashboard_url, dashboard_version

    ray_version = await dashboard_version()
    ray_status = {
        "ready": ray_version is not None,
        "address": settings.ray_address,
        "dashboard": dashboard_url(),
        "detail": (
            f"ray {ray_version.get('ray_version')}"
            if ray_version
            else "dashboard unreachable"
        ),
    }

    return {
        "status": "ok" if (temporal_ok and memgraph_ok) else "degraded",
        "temporal": {
            "connected": temporal_ok,
            "host": settings.temporal_host,
            "namespace": settings.temporal_namespace,
            "task_queue": settings.temporal_task_queue,
        },
        # Dispatch a real task rather than reporting "initialised": only an
        # executed round-trip proves the compute grid works.
        "ray": {"address": settings.ray_address, **ray_status},
        "memgraph": {"connected": memgraph_ok, "uri": settings.memgraph_uri},
        "kafka": {
            "brokers": settings.kafka_brokers,
            "topic": settings.kafka_topic_traffic,
        },
        "fleet": fleet_info,
        "activities": describe_activities(),
    }


@app.get("/", tags=["ops"], summary="Service banner")
async def root() -> dict:
    return {
        "service": settings.app_name,
        "version": "0.1.0",
        "docs": "/docs",
        "health": "/health",
        "route": "POST /api/v1/route",
    }
