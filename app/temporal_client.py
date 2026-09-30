"""Temporal client bootstrap shared by the API and the worker.

The namespace is created on demand so a fresh ``docker compose up`` works
without a manual ``temporal operator namespace create``.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import ray
from temporalio.api.workflowservice.v1 import request_response_pb2 as _wr
from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError, RPCStatusCode

from .config import Settings, get_settings
from .workflows.itinerary import ItineraryWorkflow

log = logging.getLogger("payanam.temporal")

_CLIENT: Optional[Client] = None


async def connect_temporal(settings: Optional[Settings] = None) -> Client:
    """Connect to the Temporal frontend, retrying while the stack boots."""
    global _CLIENT
    settings = settings or get_settings()
    if _CLIENT is not None:
        return _CLIENT

    last: Optional[Exception] = None
    for attempt in range(1, 11):
        try:
            client = await Client.connect(
                settings.temporal_host,
                namespace=settings.temporal_namespace,
                wait_for_ready=False,
            )
            _CLIENT = client
            log.info(
                "temporal client connected to %s (ns=%s)",
                settings.temporal_host,
                settings.temporal_namespace,
            )
            return client
        except (RPCError, OSError, RuntimeError) as exc:
            last = exc
            log.warning(
                "temporal connect attempt %s/10 failed: %s", attempt, exc
            )
            import asyncio

            await asyncio.sleep(2.0)
    raise RuntimeError(f"could not connect to temporal at {settings.temporal_host}: {last}")


def get_temporal_client() -> Client:
    if _CLIENT is None:
        raise RuntimeError("temporal client not initialised; call connect_temporal()")
    return _CLIENT

async def ensure_namespace(client: Client, namespace: str) -> bool:
    """Create the namespace if the auto-setup server does not have it yet.

    Best-effort: a fresh ``docker compose up`` gets a working namespace without
    a manual ``temporal operator namespace create``.
    """
    service = client.workflow_service

    try:
        await service.describe_namespace(
            _wr.DescribeNamespaceRequest(id=namespace, namespace=namespace)
        )
        log.info("temporal namespace '%s' already exists", namespace)
        return True
    except RPCError as exc:
        if exc.status and exc.status.code == RPCStatusCode.NOT_FOUND:
            log.info("temporal namespace '%s' not found; registering", namespace)
        else:
            log.warning("namespace describe failed: %s", exc)
            return False

    try:
        await service.register_namespace(
            _wr.RegisterNamespaceRequest(
                namespace=namespace,
                description="payanam routing namespace",
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


# --------------------------------------------------------------------------- #
# Ray
# --------------------------------------------------------------------------- #
_RAY_READY = False


def init_ray(settings: Optional[Settings] = None) -> bool:
    """Connect to the Ray cluster, falling back to a local embedded runtime.

    Never raises: routing degrades to an in-process CP-SAT solve if Ray is
    unavailable, so the API stays up.
    """
    global _RAY_READY
    settings = settings or get_settings()
    if _RAY_READY:
        return True
    try:
        if not ray.is_initialized():
            if settings.ray_address in ("local", "", None):
                ray.init(
                    num_cpus=settings.ray_num_cpus,
                    include_dashboard=False,
                    log_to_driver=False,
                    ignore_reinit_error=True,
                )
            else:
                ray.init(address=settings.ray_address, ignore_reinit_error=True)
        _RAY_READY = True
        log.info("ray ready at %s", ray.get_runtime_context().get_node_address())
        return True
    except Exception as exc:  # noqa: BLE001 - degrade, do not crash the API
        log.error("ray init failed (%s); solver will run in-process", exc)
        _RAY_READY = False
        return False


def ray_ready() -> bool:
    return _RAY_READY and ray.is_initialized()


def shutdown_ray() -> None:
    global _RAY_READY
    if ray.is_initialized():
        ray.shutdown()
    _RAY_READY = False

