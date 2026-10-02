"""Temporal client bootstrap shared by the API and the worker.

The namespace is created on demand so a fresh ``docker compose up`` works
without a manual ``temporal operator namespace create``.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Dict, Optional

from temporalio.api.workflowservice.v1 import request_response_pb2 as _wr
from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError, RPCStatusCode

from .config import Settings, get_settings
from .workflows.itinerary import ItineraryWorkflow

log = logging.getLogger("payanam.temporal")

_CLIENT: Optional[Client] = None


async def ray_health_probe(timeout: float = 5.0) -> Dict[str, object]:
    """Report whether the Ray Job dashboard is reachable.

    The API process no longer holds a Ray context -- solving is dispatched over
    the Ray Jobs REST API from :mod:`app.solver.ray_dispatch`. So the health
    signal is the dashboard answering, not a locally initialised runtime.
    """
    from .solver.ray_dispatch import dashboard_version

    version = await dashboard_version()
    if version is None:
        return {"ready": False, "detail": "ray dashboard unreachable"}
    return {
        "ready": True,
        "detail": f"dashboard {version.get('ray_version', '?')}",
    }


async def connect_temporal(settings: Optional[Settings] = None) -> Client:
    """Connect to the Temporal frontend.

    A single attempt: readiness retrying lives in :func:`app.startup.retry_async`,
    which the lifespan and worker both call. Keeping the retry in one place
    avoids two overlapping loops with different budgets.

    ``wait_for_ready`` is deliberately NOT passed -- it is a parameter of
    ``WorkflowHandle.start_workflow``, not of ``Client.connect``. Passing it
    raised ``TypeError`` at startup, which is how this path stayed broken for
    as long as the tests used the dev-server environment.
    """
    global _CLIENT
    settings = settings or get_settings()
    if _CLIENT is not None:
        return _CLIENT

    client = await Client.connect(
        settings.temporal_host,
        namespace=settings.temporal_namespace,
    )
    _CLIENT = client
    log.info(
        "temporal client connected to %s (ns=%s)",
        settings.temporal_host,
        settings.temporal_namespace,
    )
    return client


def get_temporal_client() -> Client:
    """Return the process-wide client, initialising it if necessary."""
    global _CLIENT
    if _CLIENT is None:
        raise RuntimeError("temporal client not initialised; call connect_temporal()")
    return _CLIENT


async def ensure_namespace(client: Client, namespace: str) -> bool:
    """Create the namespace if the server does not have it yet.

    Best-effort: a fresh ``docker compose up`` gets a working namespace without
    a manual ``temporal operator namespace create``. Only genuine RPC failures
    are reported; nothing here silently downgrades the client.
    """
    from temporalio.api.workflowservice.v1 import request_response_pb2 as _wr

    service = client.workflow_service

    try:
        await service.describe_namespace(
            # Only `namespace` is set. `id` is the namespace *UUID*; passing the
            # name there fails with "invalid UUID length" and the namespace is
            # then never created.
            _wr.DescribeNamespaceRequest(namespace=namespace)
        )
        log.info("temporal namespace '%s' already exists", namespace)
        return True
    except RPCError as exc:
        # RPCError.status *is* the RPCStatusCode enum member; it has no .code
        # attribute. Reading .code raised AttributeError and crashed startup.
        if exc.status == RPCStatusCode.NOT_FOUND:
            log.info("temporal namespace '%s' not found; registering", namespace)
        else:
            log.warning("namespace describe failed: %s", exc)
            return False

    try:
        await service.register_namespace(
            _wr.RegisterNamespaceRequest(
                namespace=namespace,
                description="Payanam routing namespace",
                workflow_execution_retention_period=timedelta(days=3),
            )
        )
        log.info("registered temporal namespace '%s'", namespace)
        return True
    except RPCError as exc:
        # ALREADY_EXISTS is a benign race between API replicas.
        if "already exists" in str(exc).lower():
            return True
        log.warning("namespace registration failed: %s", exc)
        return False


async def start_itinerary_workflow(
    client: Client,
    settings: Settings,
    itinerary_id: str,
    legs: list,
    request_payload: Optional[dict] = None,
    waitlist_probability: float = 0.0,
    replan_grace_seconds: float = 0.0,
) -> WorkflowHandle:
    """Start the Saga on the routing task queue."""
    workflow_id = f"{settings.temporal_workflow_id_prefix}{itinerary_id}"
    return await client.start_workflow(
        ItineraryWorkflow.run,
        args=[
            itinerary_id,
            legs,
            request_payload,
            waitlist_probability,
            replan_grace_seconds,
        ],
        id=workflow_id,
        task_queue=settings.temporal_task_queue,
    )


