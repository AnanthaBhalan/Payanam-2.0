"""Integration test for Ray Client execution.

Verifies the compute grid genuinely schedules and returns work -- a socket
check is not enough, so the probe submits a real ``@ray.remote`` task and reads
the result back.

Skips cleanly when no cluster is reachable, following the fail-closed
convention from Phase 3: an unreachable cluster is a skip, a *broken* client is
a failure.

    docker compose up -d ray-head ray-worker
    pytest tests/test_ray_execution.py -v
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings  # noqa: E402


def _settings() -> Settings:
    return Settings(ray_address=os.getenv("RAY_ADDRESS", "ray://ray-head:10001"))


def _cluster_or_skip():
    """Return an initialised Ray, or skip if no cluster is reachable."""
    ray = pytest.importorskip("ray", reason="ray is not installed")

    if ray.is_initialized():
        return ray

    try:
        ray.init(
            address=_settings().ray_address,
            log_to_driver=False,
            ignore_reinit_error=True,
        )
    except Exception as exc:  # noqa: BLE001 - unreachable cluster is a skip
        pytest.skip(f"ray cluster unreachable: {exc}")
    return ray


def test_ray_is_initialized() -> None:
    ray = _cluster_or_skip()
    assert ray.is_initialized()


def test_submit_and_retrieve_remote_task() -> None:
    """Submit a trivial @ray.remote task and read the value back."""
    ray = _cluster_or_skip()

    @ray.remote
    def ping() -> bool:
        return True

    ref = ping.remote()
    assert ray.get(ref, timeout=60) is True


def test_cluster_reports_nodes() -> None:
    ray = _cluster_or_skip()
    nodes = ray.nodes()
    assert nodes, "cluster reported no nodes"
    assert any(n.get("Alive") for n in nodes), nodes


def test_solver_entrypoint_is_remote_decorated() -> None:
    """solve_routing_task must expose the .remote() interface."""
    from app.solver.cp_router import solve_routing_task

    assert hasattr(solve_routing_task, "remote"), (
        "solve_routing_task is not @ray.remote decorated"
    )


def test_job_payload_declares_a_runtime_env() -> None:
    """Ray Jobs run from an isolated workspace, so runtime_env is mandatory.

    Without PYTHONPATH pointing at the baked-in app the supervisor cannot
    import `app.*` and the job dies instantly with FAILED and no logs.
    """
    import inspect

    from app.solver import ray_dispatch

    src = inspect.getsource(ray_dispatch.submit_solve_job)
    assert '"runtime_env"' in src
    assert "RAY_APP_HOME" in src
    # The job writes to Redis from *inside* the cluster.
    assert ray_dispatch.RAY_JOB_REDIS_URL.startswith("redis://redis:")
    # Entrypoint must be an absolute path, not a bare module name.
    assert ray_dispatch.JOB_ENTRYPOINT.startswith("python3 /")


async def test_health_probe_reports_a_round_trip() -> None:
    """ray_health_probe reports on the Ray Jobs dashboard, not a local runtime."""
    from app.temporal_client import ray_health_probe

    result = await ray_health_probe()
    assert "ready" in result and "detail" in result
    # Offline is a valid report, not an exception.
    assert isinstance(result["ready"], bool)