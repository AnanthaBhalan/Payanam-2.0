"""Temporal worker: executes ItinerarySaga and its activities.

Run standalone with::

    python -m app.worker
"""
from __future__ import annotations

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from .config import get_settings
from .startup import retry_async
from .temporal_client import ensure_namespace
from .workflows.activities import ACTIVITIES, prime_redis_url
from .workflows.itinerary import ItineraryWorkflow

log = logging.getLogger("payanam.worker")


async def main() -> None:  # pragma: no cover - long-running process
    settings = get_settings()
    # Capture the Redis URL now: publish_itinerary_update runs inside the
    # Temporal sandbox, which forbids reading os.environ.
    prime_redis_url(settings.redis_url)
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    log.info("worker connecting to temporal at %s", settings.temporal_host)

    # NOTE: Ray is deliberately NOT initialised here. Ray Client binds its
    # session to the calling thread, so an init on this main thread leaves every
    # submission from the activity dispatch thread holding an unresolved
    # `InProgressSentinel`. `app.solver.ray_dispatch` initialises Ray inside the
    # single thread that owns all submissions.

    # Wait for the Temporal frontend rather than exiting: in Compose the
    # worker usually wins the race against the server's first boot, and a bare
    # failure here becomes a restart loop.
    async def _connect():
        return await Client.connect(
            settings.temporal_host, namespace=settings.temporal_namespace
        )

    client = await retry_async("temporal (worker)", _connect, settings=settings)
    if client is None:
        log.error("worker cannot start: temporal unreachable")
        return

    # The worker and the API start concurrently in the same container, so the
    # worker cannot assume the API already created the namespace -- Worker.run()
    # validates the namespace and aborts if it is missing. Registering here is
    # idempotent (ALREADY_EXISTS is treated as success).
    try:
        await ensure_namespace(client, settings.temporal_namespace)
    except Exception as exc:  # noqa: BLE001 - surfaced by Worker.run if truly absent
        log.warning("namespace ensure failed (continuing): %s", exc)

    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[ItineraryWorkflow],
        activities=list(ACTIVITIES),
    )
    log.info(
        "worker polling '%s' for %d activities",
        settings.temporal_task_queue,
        len(ACTIVITIES),
    )
    await worker.run()


if __name__ == "__main__":  # pragma: no cover
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("worker interrupted")
