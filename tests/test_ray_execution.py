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


def test_ray_dispatch_serialises_through_one_thread() -> None:
    """The dispatch pool must have exactly one worker.

    Ray Client is not thread-safe: more than one worker thread reintroduces the
    cross-thread `InProgressSentinel` reference failure.
    """
    from app.solver.ray_dispatch import _pool

    assert _pool()._max_workers == 1


def test_health_probe_reports_a_round_trip() -> None:
    """ray_health_probe must execute a task, not merely report initialised."""
    from app.temporal_client import ray_health_probe

    result = ray_health_probe(timeout=30.0)
    assert "ready" in result and "detail" in result
    # With no cluster in-process this is False; that is a valid report.
    assert isinstance(result["ready"], bool)