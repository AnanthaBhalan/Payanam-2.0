"""Startup dependency waiting, shared by the API lifespan and the worker.

Container start order is not guaranteed: the API and worker routinely boot
while Memgraph, Temporal and Redis are still initialising. Without a retry the
process would exit and Docker would restart it in a loop while the real
dependency was still coming up.

Design notes
------------
* Retrying is deliberately **narrow**. Only transport-level failures
  (connection refused, DNS, timeouts) are retried. A ``ClientError`` from bad
  Cypher or an ``AttributeError`` from a missing method is a *bug*, and
  retrying it just delays the failure while hiding the cause -- consistent with
  the fail-closed policy in :mod:`app.graph.repository`.
* Exhausting the budget does not silently continue. Either the process fails
  loudly (``startup_fail_fast``) or it proceeds with ``/health`` reporting
  ``degraded`` so an orchestrator can gate traffic. Neither path crashes-loops.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Iterable, Type

from .config import Settings, get_settings

log = logging.getLogger("payanam.startup")

# Errors that mean "the dependency is not up yet". Deliberately specific.
RETRYABLE: tuple[Type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
    asyncio.TimeoutError,
)

try:  # neo4j driver errors that indicate an unreachable server
    from neo4j.exceptions import ServiceUnavailable, SessionExpired

    RETRYABLE = RETRYABLE + (ServiceUnavailable, SessionExpired)
except ImportError:  # pragma: no cover - neo4j is a hard dependency
    pass

try:  # redis connection failures
    from redis.exceptions import ConnectionError as RedisConnectionError

    RETRYABLE = RETRYABLE + (RedisConnectionError,)
except ImportError:  # pragma: no cover
    pass


async def retry_async(
    name: str,
    factory: Callable[[], Awaitable[Any]],
    *,
    settings: Settings | None = None,
    required: bool = True,
    extra_retryable: Iterable[Type[BaseException]] = (),
) -> Any:
    """Await ``factory()``, retrying only while the dependency is unreachable.

    Returns the value on success. On exhaustion: raises if ``required`` and
    ``startup_fail_fast``; otherwise returns ``None`` and logs loudly.
    """
    settings = settings or get_settings()
    retryable = RETRYABLE + tuple(extra_retryable)
    attempts = settings.startup_max_attempts
    delay = settings.startup_backoff_seconds

    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            value = await factory()
        except retryable as exc:
            last = exc
            log.warning(
                "%s not ready (attempt %s/%s): %s", name, attempt, attempts, exc
            )
            if attempt < attempts:
                await asyncio.sleep(delay)
            continue
        except Exception as exc:  # noqa: BLE001 - a real bug, not a slow start
            # Fail closed: surface immediately rather than masking a defect
            # behind a retry loop.
            log.error("%s failed with a non-transient error: %s", name, exc)
            raise
        else:
            if attempt > 1:
                log.info("%s became ready after %s attempt(s)", name, attempt)
            return value

    log.error("%s unreachable after %s attempt(s): %s", name, attempts, last)
    if required and settings.startup_fail_fast:
        raise RuntimeError(
            f"{name} unreachable after {attempts} attempt(s): {last}"
        ) from last
    return None