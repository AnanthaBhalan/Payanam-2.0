"""Redis Pub/Sub -> SSE translation tests.

Fail-closed: every test skips cleanly when Redis is unreachable, so the suite
still runs in environments with no containers.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

REDIS_URL = "redis://localhost:6389/0"  # isolated .env profile, from the host

# The activity and the SSE route both read get_settings().redis_url, which
# defaults to :6379. Point it at the isolated port for the duration of the run
# so the test exercises the same code path the API container uses.
os.environ.setdefault("REDIS_URL", REDIS_URL)

# get_settings() is @lru_cache'd, so if another test module in the same pytest
# process resolved settings before REDIS_URL was set, this module would inherit
# the stale :6379 value. Clear the cache so the env var above takes effect.
from app.config import get_settings as _get_settings  # noqa: E402

_get_settings.cache_clear()


def _client():
    import redis.asyncio as aioredis

    return aioredis.from_url(REDIS_URL, decode_responses=True)


async def _redis_or_skip():
    client = _client()
    try:
        await client.ping()
    except Exception as exc:  # noqa: BLE001 - unreachable is a skip
        await client.aclose()
        pytest.skip(f"redis unreachable at {REDIS_URL}: {exc}")
    return client


async def test_publish_activity_reaches_a_subscriber() -> None:
    """publish_itinerary_update lands on payanam:updates:{workflow_id}."""
    from app.workflows.activities import publish_itinerary_update

    sub = await _redis_or_skip()
    pub = _client()
    workflow_id = "wf-pubsub-1"
    channel = f"payanam:updates:{workflow_id}"
    ps = sub.pubsub()
    try:
        await ps.subscribe(channel)
        await asyncio.sleep(0.2)

        receipt = await publish_itinerary_update(
            {"workflow_id": workflow_id, "status": "re-routing", "new_mode": "CAB"}
        )
        assert receipt["channel"] == channel
        # workflow_id is the channel, so it is stripped from the payload.
        assert "workflow_id" not in receipt["event"]

        # The subscription may need a beat to register; poll rather than assume.
        msg = None
        for _ in range(20):
            msg = await ps.get_message(ignore_subscribe_messages=True, timeout=0.5)
            if msg is not None:
                break
        assert msg is not None, "no pub/sub message received"
        event = json.loads(msg["data"])
        assert event["status"] == "re-routing"
        assert event["new_mode"] == "CAB"
    finally:
        await ps.aclose()
        await pub.aclose()
        await sub.aclose()


async def test_publish_requires_workflow_id() -> None:
    """A missing workflow_id is a terminal ApplicationError, not a silent no-op."""
    from temporalio.exceptions import ApplicationError

    from app.workflows.activities import publish_itinerary_update

    await _redis_or_skip()
    with pytest.raises(ApplicationError):
        await publish_itinerary_update({"status": "re-routing"})


async def _drain_until(agen, predicate, timeout: float = 15.0):
    """Pull SSE frames from the endpoint's generator until `predicate` matches.

    Drives the async generator directly rather than through TestClient: an
    endless StreamingResponse deadlocks TestClient's blocking portal.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    seen_ready = False
    try:
        while loop.time() < deadline:
            remaining = max(0.05, deadline - loop.time())
            frame = await asyncio.wait_for(agen.__anext__(), timeout=remaining)
            if frame.startswith("event: ready"):
                seen_ready = True
            if predicate(frame):
                return True, seen_ready
    except (asyncio.TimeoutError, StopAsyncIteration):
        pass
    return False, seen_ready


async def test_sse_route_translates_pubsub_to_event_stream() -> None:
    """A message published to Redis surfaces as `data: {...}` from the route."""
    from app.api.routes import stream_itinerary

    await _redis_or_skip()
    workflow_id = "wf-sse-1"
    channel = f"payanam:updates:{workflow_id}"
    pub = _client()

    async def publish_later() -> None:
        await asyncio.sleep(1.5)
        await pub.publish(
            channel, json.dumps({"status": "re-routing", "new_mode": "CAB"})
        )

    # A short heartbeat so the idle branch re-polls Pub/Sub promptly instead of
    # sleeping past the publish.
    resp = await stream_itinerary(workflow_id=workflow_id, heartbeat_seconds=1.0)
    assert resp.media_type == "text/event-stream"
    assert resp.headers["x-accel-buffering"] == "no"

    agen = resp.body_iterator
    task = asyncio.ensure_future(publish_later())
    try:
        hit, seen_ready = await _drain_until(
            agen, lambda f: f.startswith("data: ") and '"re-routing"' in f
        )
    finally:
        task.cancel()
        await agen.aclose()
        await pub.aclose()

    assert seen_ready, "no 'ready' event received"
    assert hit, "pubsub message did not reach the SSE stream"


async def test_sse_emits_keepalive_comment_when_idle() -> None:
    """An idle stream still yields `: keep-alive` so proxies do not hang up."""
    from app.api.routes import stream_itinerary

    await _redis_or_skip()
    resp = await stream_itinerary(workflow_id="wf-sse-idle", heartbeat_seconds=1.0)
    agen = resp.body_iterator
    try:
        hit, _ = await _drain_until(agen, lambda f: f.startswith(": keep-alive"), 6.0)
    finally:
        await agen.aclose()
    assert hit, "no keep-alive comment received"


async def test_sse_stream_terminates_cleanly_on_client_disconnect() -> None:
    """Cancelling the generator runs its `finally` teardown without leaking."""
    from app.api.routes import stream_itinerary

    await _redis_or_skip()
    resp = await stream_itinerary(workflow_id="wf-sse-cancel", heartbeat_seconds=15.0)
    agen = resp.body_iterator
    first = await asyncio.wait_for(agen.__anext__(), timeout=5.0)
    assert first.startswith("event: ready")
    await agen.aclose()  # must not raise
