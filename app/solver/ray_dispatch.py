"""Ray Job Submission over HTTP -- no Ray Client, no thread-safety constraints.

The previous implementation drove ``@ray.remote`` through Ray Client
(``ray://``), which binds its session to the calling thread and is incompatible
with Temporal's concurrent activity execution. Every submission from a
temporal activity failed with::

    AttributeError: 'InProgressSentinel' object has no attribute 'id'

This module replaces it with the supported integration path for an external
orchestrator: the Ray Job **REST API** on the dashboard.

1. ``submit_solve_job`` POSTs to ``/api/jobs/`` with an ``entrypoint`` running
   ``app/solver/job_entrypoint.py`` inside the cluster. The job writes its
   result to Redis; nothing streams back over the HTTP channel.
2. ``wait_for_solve_result`` polls ``/api/jobs/{id}`` with bounded backoff and,
   on ``SUCCEEDED``, reads ``payanam:solve_result:{task_id}`` from Redis.
3. ``FAILED``/``STOPPED`` raise so the activity's retry policy applies.

This module imports no ray at all, so the Temporal worker never touches the
Ray client library.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import uuid
from typing import Any, Dict, Optional

import httpx

log = logging.getLogger("payanam.solver.jobs")

DEFAULT_DASHBOARD = "http://ray-head:8265"
RESULT_TTL_SECONDS = 600

# Entrypoint executed by the cluster. The Ray image bakes the app to /opt/payanam.
JOB_ENTRYPOINT = "python3 /opt/payanam/app/solver/job_entrypoint.py"

# Backoff schedule (seconds); the last value repeats.
POLL_BACKOFF = (1.0, 2.0, 3.0, 5.0, 8.0, 10.0)


def result_key(task_id: str) -> str:
    """Redis key holding the solver output for ``task_id``."""
    return f"payanam:solve_result:{task_id}"


def dashboard_url(settings: Any = None) -> str:
    """Dashboard base URL for the Ray Job REST API.

    Precedence: ``RAY_DASHBOARD_URL`` env var, then derived from
    ``RAY_ADDRESS`` (``ray://host:10001`` -> ``http://host:8265``). The env
    override matters when running outside the compose network, where
    ``ray-head`` is not resolvable.
    """
    import os

    override = os.getenv("RAY_DASHBOARD_URL")
    if override:
        return override.rstrip("/")

    if settings is None:
        from ..config import get_settings

        settings = get_settings()
    addr = getattr(settings, "ray_address", "") or ""
    if addr.startswith("ray://"):
        host = addr[len("ray://") :].split(":", 1)[0]
        return f"http://{host}:8265"
    return DEFAULT_DASHBOARD


async def dashboard_version(timeout: float = 5.0) -> Optional[Dict[str, Any]]:
    """Return the Ray dashboard version payload, or None if unreachable.

    Used by /health: confirms the dashboard answers without starting Ray Client
    or executing a task.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{dashboard_url()}/api/version")
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:  # noqa: BLE001 - health must never raise
        log.debug("dashboard unreachable: %s", exc)
        return None


async def submit_solve_job(
    payload: Dict[str, Any],
    task_id: Optional[str] = None,
    time_limit_seconds: float = 5.0,
    redis_url: Optional[str] = None,
    timeout: float = 30.0,
) -> str:
    """POST a solve job and return its submission id.

    Raises on any transport or API error so the activity fails loudly -- the
    fail-closed policy; never silently solve in-process instead.
    """
    from ..config import get_settings

    settings = get_settings()
    task_id = task_id or uuid.uuid4().hex
    redis_url = redis_url or settings.redis_url

    encoded = base64.b64encode(
        json.dumps(payload, default=str).encode("utf-8")
    ).decode("ascii")

    entrypoint = (
        f"{JOB_ENTRYPOINT} --task-id {task_id} "
        f"--payload-b64 {encoded} --time-limit {time_limit_seconds} "
        f"--redis-url {redis_url}"
    )
    body = {
        "entrypoint": entrypoint,
        "job_id": task_id,
        "metadata": {"task_id": task_id, "kind": "cp_sat_solve"},
    }

    dashboard = dashboard_url(settings)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(f"{dashboard}/api/jobs/", json=body)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"ray job submission failed ({resp.status_code}): {resp.text[:300]}"
            )
        data = resp.json()

    job_id = data.get("id") or data.get("submission_id")
    if not job_id:
        raise RuntimeError(f"ray job submission returned no id: {data}")
    log.info("submitted ray job task_id=%s job_id=%s", task_id, job_id)
    return job_id


async def wait_for_solve_result(
    job_id: str,
    task_id: str,
    redis_client: Any,
    overall_timeout: float = 300.0,
) -> Dict[str, Any]:
    """Poll until the job finishes, then read the result from Redis.

    Returns the solver output. Raises ``RuntimeError`` if the job fails or the
    budget is exhausted.
    """
    dashboard = dashboard_url()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + overall_timeout

    async with httpx.AsyncClient(timeout=15.0) as client:
        idx = 0
        last_status = "UNKNOWN"
        while loop.time() < deadline:
            status = last_status
            try:
                resp = await client.get(f"{dashboard}/api/jobs/{job_id}")
                resp.raise_for_status()
                status = (resp.json() or {}).get("status", "UNKNOWN")
            except Exception as exc:  # noqa: BLE001 - transient poll errors
                log.debug("poll error for %s: %s", job_id, exc)

            if status != last_status:
                log.info("ray job %s -> %s", job_id, status)
                last_status = status

            if status == "SUCCEEDED":
                raw = await redis_client.get(result_key(task_id))
                if raw:
                    log.info("retrieved solve result for task_id=%s", task_id)
                    return json.loads(raw)
                log.debug("job succeeded; result key not yet present")

            if status in ("FAILED", "STOPPED"):
                raise RuntimeError(
                    f"ray job {job_id} ended with status {status}"
                )

            await asyncio.sleep(POLL_BACKOFF[min(idx, len(POLL_BACKOFF) - 1)])
            idx += 1

    raise RuntimeError(
        f"ray job {job_id} did not complete within {overall_timeout}s "
        f"(last={last_status})"
    )
