"""Serialised Ray Client dispatch.

Why this module exists
----------------------
Ray Client (``ray://``) is **not thread-safe**. A task reference created in one
thread resolves to an ``InProgressSentinel`` in another, producing::

    AttributeError: 'InProgressSentinel' object has no attribute 'id'

Temporal runs activities concurrently in the event loop, from a context
different from the one that called ``ray.init()``. The fix is to confine every
Ray call to a single dedicated worker thread that owns the client for the
lifetime of the process, and marshal results back to the event loop.

The pool has exactly one worker on purpose: more workers would reintroduce the
cross-thread reference problem this is meant to eliminate.
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict

log = logging.getLogger("payanam.solver.ray")

# Single worker => the Ray Client is only ever touched by one thread.
_executor: ThreadPoolExecutor | None = None


def _pool() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ray-client"
        )
        log.info("ray dispatch pool created (single worker)")
    return _executor


def _submit_sync(
    remote_fn: Callable[..., Any], payload: Dict[str, Any], time_limit: float
) -> Dict[str, Any]:
    """Blocking body executed on the Ray-owning thread.

    ``ray.init()`` is called *here*, in this thread, rather than at process
    startup: Ray Client binds its session to the thread that initialised it, so
    an init on the main thread leaves every submission from a worker thread
    holding an unresolved ``InProgressSentinel``.
    """
    import ray

    from ..config import get_settings

    settings = get_settings()
    if not ray.is_initialized():
        ray.init(
            address=settings.ray_address,
            log_to_driver=False,
            ignore_reinit_error=True,
        )
        log.info("ray initialised on the dispatch thread")

    ref = remote_fn.remote(payload, time_limit)
    return ray.get(ref)


async def submit_solve(
    remote_fn: Callable[..., Any],
    payload: Dict[str, Any],
    time_limit_seconds: float = 5.0,
) -> Dict[str, Any]:
    """Dispatch ``remote_fn`` to the cluster and await its result.

    Raises whatever Ray raises, so the activity's own retry/failure semantics
    apply unchanged (fail-closed: no silent in-process fallback here).
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _pool(), _submit_sync, remote_fn, payload, time_limit_seconds
    )


def shutdown() -> None:
    """Release the pool (called on application shutdown)."""
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=False)
        _executor = None