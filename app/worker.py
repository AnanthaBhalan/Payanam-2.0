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
from .workflows.activities import ACTIVITIES
from .workflows.itinerary import ItineraryWorkflow

log = logging.getLogger("payanam.worker")


async def main() -> None:  # pragma: no cover - long-running process
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    log.info("worker connecting to temporal at %s", settings.temporal_host)

    client = await Client.connect(
        settings.temporal_host, namespace=settings.temporal_namespace
    )
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
