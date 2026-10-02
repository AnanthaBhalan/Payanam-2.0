"""Integration test for Ray Job Submission + the Redis result handoff.

Submits a real CP-SAT job through the Ray Job REST API and awaits the result
that the cluster writes to Redis. This is the replacement for Ray Client
dispatch, which cannot be used from Temporal activities.

Skips cleanly when the dashboard (ray-head:8265) or Redis is unreachable,
following the fail-closed convention established in Phase 3.
"""
from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.fleet.state import FleetState  # noqa: E402
from app.sample_data import sample_request  # noqa: E402
from app.solver.ray_dispatch import (  # noqa: E402
    dashboard_url,
    dashboard_version,
    result_key,
    submit_solve_job,
    wait_for_solve_result,
)

REDIS_URL = "redis://localhost:6389/0"  # the isolated .env profile


async def _fleet_or_skip() -> FleetState:
    import redis.asyncio as aioredis

    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except Exception as exc:  # noqa: BLE001 - unreachable is a skip
        pytest.skip(f"redis unreachable at {REDIS_URL}: {exc}")
    return FleetState(client)


async def test_dashboard_reachable() -> None:
    version = await dashboard_version()
    if version is None:
        pytest.skip(f"ray dashboard unreachable at {dashboard_url()}")
    assert "ray_version" in version


async def test_submit_job_and_read_result_from_redis() -> None:
    """Full handoff: REST submit -> cluster solve -> Redis result."""
    version = await dashboard_version()
    if version is None:
        pytest.skip(f"ray dashboard unreachable at {dashboard_url()}")
    fleet = await _fleet_or_skip()
    redis_client = fleet.redis

    task_id = uuid.uuid4().hex
    key = result_key(task_id)
    payload = sample_request().model_dump(mode="json")

    try:
        job_id = await submit_solve_job(payload, task_id=task_id, time_limit_seconds=5.0)
        assert job_id, "no submission id returned"

        result = await wait_for_solve_result(
            job_id, task_id, redis_client, overall_timeout=240.0
        )

        assert result.get("feasible") is True, result
        assert isinstance(result.get("visits"), list)
        for visit in result["visits"]:
            assert "node_id" in visit and "arrival_min" in visit
    finally:
        # Mandate: clean up the handoff key after asserting.
        await redis_client.delete(key)
        assert await redis_client.get(key) is None, "handoff key was not cleaned up"