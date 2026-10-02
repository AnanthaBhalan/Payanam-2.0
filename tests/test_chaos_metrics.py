"""Bounded, deterministic version of scripts/chaos_monkey.py.

The real harness spawns 50 tourists and needs the whole stack. This suite
scales it to 5 workflows / 2 sabotaged and asserts the Saga recovers each one
through the cab fallback.

Two tiers:
  * ledger tests -- pure, no I/O, always run; lock the metric semantics.
  * live tests   -- skip unless the full stack is up (fail-closed convention).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

API_BASE = os.environ.get("PAYANAM_API", "http://localhost:8001")
KAFKA_BOOTSTRAP = os.environ.get("PAYANAM_KAFKA", "localhost:9095")

TOURISTS = 5
SABOTAGE_RATE = 0.4  # -> exactly 2 of 5

# The live tests below assert the FULL chain: Kafka -> aiokafka -> Temporal
# signal -> Saga unwind -> Redis GEO lock -> cab -> SSE. Phase 10 made this
# work by moving the Redis broadcast out of the sandbox, so they now run
# whenever the stack is up (and skip cleanly when it is not).
# The live tests below assert the FULL chain: Kafka -> aiokafka -> Temporal
# signal -> Saga unwind -> Redis GEO lock -> cab -> SSE. Phase 10 made this
# work by moving the Redis broadcast out of the workflow sandbox, so they run
# by default and still skip cleanly when the stack is not up.
LIVE = os.environ.get("PAYANAM_CHAOS_LIVE", "1") == "1"
requires_live = pytest.mark.skipif(
    not LIVE,
    reason='set PAYANAM_CHAOS_LIVE=1 and start the stack to run the full chain',
)


# --------------------------------------------------------------------------- #
# Tier 1: ledger semantics (always runs)
# --------------------------------------------------------------------------- #
async def test_ledger_tracks_disrupt_and_recover() -> None:
    from chaos_monkey import Ledger

    ledger = Ledger()
    ledger.total = 3
    await ledger.mark_sabotaged("wf-1")
    await ledger.mark_sabotaged("wf-2")

    await ledger.observe("wf-1", {"status": "re-routing", "new_mode": "CAB"})
    assert "wf-1" in ledger.disrupted
    assert ledger.pending() == {"wf-1", "wf-2"}

    await ledger.observe("wf-1", {"status": "fallback_secured", "mode": "CAB"})
    assert ledger.pending() == {"wf-2"}

    report = await ledger.report()
    assert report["recovered_trips"] == 1
    assert report["avg_recovery_latency_s"] is not None


async def test_ledger_records_fallback_failure_separately() -> None:
    """A failed fallback must NOT count as recovered."""
    from chaos_monkey import Ledger

    ledger = Ledger()
    await ledger.mark_sabotaged("wf-x")
    await ledger.observe("wf-x", {"status": "fallback_failed", "reason": "no fleet"})
    report = await ledger.report()
    assert report["recovered_trips"] == 0
    assert report["failed_trips"] == 1
    assert ledger.pending() == set()


async def test_sabotage_rate_selects_exact_count() -> None:
    """The saboteur's sample size is deterministic under a fixed seed."""
    import random

    ids = [f"wf-{i}" for i in range(TOURISTS)]
    rng = random.Random(1337)
    k = max(1, int(round(len(ids) * SABOTAGE_RATE)))
    chosen = rng.sample(ids, k)
    assert len(chosen) == 2
    assert len(set(chosen)) == 2  # no double-sabotage




# --------------------------------------------------------------------------- #
# Tier 2: live stack (skips cleanly when unavailable)
# --------------------------------------------------------------------------- #
def _stack_up() -> None:
    import httpx

    try:
        r = httpx.get(f"{API_BASE}/health", timeout=8.0)
        r.raise_for_status()
    except Exception as exc:  # noqa: BLE001 - unreachable is a skip
        pytest.skip(f"stack unreachable at {API_BASE}: {exc}")


async def _kafka_bridge_live() -> bool:
    """Is the Kafka broker usable from here?

    Produces a probe on `traffic_updates`. When the containerised librdkafka
    consumer cannot resolve the broker, every transit_update is silently
    dropped and the whole chain is untestable -- so surface that as an explicit
    skip rather than a confusing downstream assertion.
    """
    import time

    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        # Awaited, not asyncio.run()'d: this executes inside the pytest-asyncio
        # loop, where asyncio.run() would raise.
        await producer.start()
        await producer.send_and_wait(
            "traffic_updates",
            json.dumps(
                {
                    "workflow_id": f"chaos-probe-{int(time.time())}",
                    "leg_id": "e_chn_tan",
                    "p_confirm": 1.0,
                }
            ).encode(),
        )
    except Exception as exc:  # noqa: BLE001 - unusable broker is a skip
        # Redpanda advertises itself as `redpanda:9092`, a name only resolvable
        # on the Docker network. A host-side client opens localhost:9095 fine
        # but cannot follow the metadata, so it cannot drive the broker. The
        # full chain is verified with `docker compose --profile chaos run`.
        pytest.skip(f"kafka not usable from this host: {type(exc).__name__}: {exc}")
    finally:
        await producer.stop()

    return True


@requires_live
async def test_five_tourists_two_sabotaged_both_recover() -> None:
    """End-to-end: 5 workflows, 2 sabotaged, both must book a cab.

    Asserts the mandates directly -- exactly 2 disruptions and exactly 2
    `fallback_secured` payloads on the SSE streams, with no dropped sockets.
    """
    from chaos_monkey import Ledger, inject_chaos, observe_stream, spawn_tourists

    _stack_up()
    await _kafka_bridge_live()

    ledger = Ledger()
    stop = asyncio.Event()

    workflow_ids = await spawn_tourists(TOURISTS, api_base=API_BASE)
    ledger.total = len(workflow_ids)
    assert ledger.total == TOURISTS, f"only {ledger.total}/{TOURISTS} started"

    observers = [
        asyncio.create_task(observe_stream(wf, ledger, API_BASE, stop))
        for wf in workflow_ids
    ]
    await asyncio.sleep(2.0)  # let the Pub/Sub subscriptions register

    try:
        targets = await inject_chaos(
            workflow_ids,
            sabotage_rate=SABOTAGE_RATE,
            bootstrap=KAFKA_BOOTSTRAP,
            ledger=ledger,
            seed=1337,
        )
        assert len(targets) == 2, f"expected 2 sabotaged, got {len(targets)}"

        loop = asyncio.get_event_loop()
        deadline = loop.time() + 60.0
        while loop.time() < deadline and ledger.pending():
            await asyncio.sleep(0.5)
    finally:
        stop.set()
        for task in observers:
            task.cancel()
        await asyncio.gather(*observers, return_exceptions=True)

    report = await ledger.report()
    print("\nchaos report:", json.dumps(report, indent=2))

    assert report["sabotaged_trips"] == 2
    assert report["recovered_trips"] == 2, f"not all recovered: {report}"
    assert report["disrupted_trips"] == 2, f"re-routing not observed: {report}"
    assert report["failed_trips"] == 0
    assert report["dropped_streams"] == 0, f"SSE dropped: {report}"


@requires_live
async def test_concurrent_fallbacks_leave_fleet_consistent() -> None:
    """Concurrent cab fallbacks must not corrupt the fleet lock state."""
    import httpx

    from chaos_monkey import inject_chaos, spawn_tourists

    _stack_up()
    await _kafka_bridge_live()

    workflow_ids = await spawn_tourists(TOURISTS, api_base=API_BASE)
    targets = await inject_chaos(
        workflow_ids, sabotage_rate=0.8, bootstrap=KAFKA_BOOTSTRAP, seed=7
    )
    assert len(targets) >= 3, "need >=3 concurrent fallbacks for this check"

    await asyncio.sleep(25.0)  # let the fallbacks settle
    r = httpx.get(f"{API_BASE}/api/v1/driver/fleet/stats", timeout=15.0)
    r.raise_for_status()
    stats = r.json()
    # Locks must be non-negative and bounded by what was ever registered; a
    # negative or absurd count would mean the WATCH/MULTI claim raced.
    assert stats.get("available", 0) >= 0
    assert 0 <= stats.get("locked", 0) <= len(targets)

async def test_recovery_latency_is_measured_from_injection() -> None:
    from chaos_monkey import Ledger

    ledger = Ledger()
    await ledger.mark_sabotaged("wf-t")
    await asyncio.sleep(0.05)
    await ledger.observe("wf-t", {"status": "fallback_secured"})
    lat = ledger.latencies()
    assert len(lat) == 1 and lat[0] >= 0.05

