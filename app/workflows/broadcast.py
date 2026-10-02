"""Redis Pub/Sub broadcast for live itinerary updates.

Phase 10: this lives in its own module on purpose.

``ItineraryWorkflow`` imports ``app.workflows.activities`` through
``workflow.unsafe.imports_passed_through()`` so it can reference activity
functions. Anything that module imports at call time is therefore evaluated
inside the Temporal sandbox, which rejects ``redis.asyncio`` with

    Cannot access threading.RLock.__typing_substitutions from inside a workflow

Keeping the Redis client out of ``activities.py`` entirely -- and invoking this
activity by its *registered string name* rather than by function reference --
removes the sandbox from the path completely. The worker registers this module
normally, so the activity body executes as ordinary unsandboxed worker code.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

import redis.asyncio as aioredis
from temporalio import activity
from temporalio.exceptions import ApplicationError

log = logging.getLogger("payanam.broadcast")

ACTIVITY_NAME = "publish_itinerary_update"

# Resolved once by the worker at startup (see prime_redis_url). os.environ is
# unreadable from sandboxed workflow code, so the activity never reads config.
_REDIS_URL: Optional[str] = None


def prime_redis_url(url: Optional[str] = None) -> str:
    """Capture the Redis URL at worker startup, before any replay can occur."""
    global _REDIS_URL
    if url is None:
        from ..config import get_settings

        url = get_settings().redis_url
    _REDIS_URL = url
    return _REDIS_URL


def _redis_url() -> str:
    if _REDIS_URL is None:  # pragma: no cover - defensive
        return prime_redis_url()
    return _REDIS_URL


@activity.defn(name=ACTIVITY_NAME)
async def publish_itinerary_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Publish one workflow state change to ``payanam:updates:{workflow_id}``."""
    workflow_id = str(payload.get("workflow_id") or "")
    if not workflow_id:
        raise ApplicationError("MISSING_WORKFLOW_ID", non_retryable=True)

    event = {k: v for k, v in payload.items() if k != "workflow_id"}
    channel = f"payanam:updates:{workflow_id}"

    client = aioredis.from_url(_redis_url(), decode_responses=True)
    try:
        receivers = await client.publish(channel, json.dumps(event, default=str))
    finally:
        await client.aclose()

    activity.logger.info("published %s -> %d subscriber(s)", channel, receivers)
    return {"channel": channel, "receivers": int(receivers), "event": event}