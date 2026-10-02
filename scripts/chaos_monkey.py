"""Chaos monkey for the Payanam stack.

Spawns N concurrent tourist workflows, sabotages a fraction of their transit
legs through Kafka, and watches the SSE streams to confirm the Temporal Saga
recovers via the geospatial fleet fallback.

    python scripts/chaos_monkey.py --tourists 50 --sabotage-rate 0.3

The metrics ledger is importable and free of side effects at module level so
``tests/test_chaos_metrics.py`` can drive a scaled-down, deterministic version
of the same logic.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import httpx
from httpx_sse import aconnect_sse

API_BASE = os.environ.get("PAYANAM_API", "http://localhost:8001")
KAFKA_BOOTSTRAP = os.environ.get("PAYANAM_KAFKA", "localhost:9095")
TRAFFIC_TOPIC = os.environ.get("PAYANAM_TOPIC", "traffic_updates")

# p_confirm forced onto a sabotaged leg. Below DEGRADED_THRESHOLD (0.15), so
# the workflow must unwind its Saga and book a cab.
SABOTAGE_P_CONFIRM = 0.05


# --------------------------------------------------------------------------- #
# Metrics ledger
# --------------------------------------------------------------------------- #
@dataclass
class Ledger:
    """Record of what the chaos run observed.

    Everything mutates on a single asyncio loop, so the plain dicts are already
    safe; the lock is held across each read-modify-write anyway to stay correct
    if this is ever driven from a thread pool.
    """

    lock: Any = field(default_factory=asyncio.Lock, repr=False)
    total: int = 0
    sabotaged: set = field(default_factory=set)
    disrupted: set = field(default_factory=set)
    recovered: set = field(default_factory=set)
    failed: set = field(default_factory=set)
    injected_at: Dict[str, float] = field(default_factory=dict)
    recovered_at: Dict[str, float] = field(default_factory=dict)
    streams_dropped: int = 0
    # Workflow ids whose SSE stream has sent its 'ready' frame (i.e. the Redis
    # subscription is registered). Chaos runs must not publish before this.
    ready_streams: set = field(default_factory=set)
    on_ready: Optional[Any] = field(default=None, repr=False)

    async def mark_ready(self, workflow_id: str) -> None:
        async with self.lock:
            self.ready_streams.add(workflow_id)
        if self.on_ready is not None:
            self.on_ready(workflow_id)

    async def mark_sabotaged(self, workflow_id: str) -> None:
        async with self.lock:
            self.sabotaged.add(workflow_id)
            self.injected_at[workflow_id] = time.monotonic()

    async def observe(self, workflow_id: str, event: Dict[str, Any]) -> None:
        """Fold one SSE event into the ledger."""
        status = event.get("status")
        async with self.lock:
            if status == "re-routing":
                self.disrupted.add(workflow_id)
            elif status == "fallback_secured":
                self.recovered.add(workflow_id)
                self.recovered_at[workflow_id] = time.monotonic()
            elif status == "fallback_failed":
                self.failed.add(workflow_id)

    async def mark_dropped(self, workflow_id: str) -> None:
        async with self.lock:
            self.streams_dropped += 1
            self.failed.add(workflow_id)

    def pending(self) -> set:
        """Sabotaged workflows that have neither recovered nor failed."""
        return self.sabotaged - self.recovered - self.failed

    def latencies(self) -> List[float]:
        return [
            self.recovered_at[w] - self.injected_at[w]
            for w in self.recovered
            if w in self.injected_at
        ]

    async def report(self) -> Dict[str, Any]:
        lat = self.latencies()
        return {
            "total_trips": self.total,
            "sabotaged_trips": len(self.sabotaged),
            "disrupted_trips": len(self.disrupted),
            "recovered_trips": len(self.recovered),
            "failed_trips": len(self.failed),
            "dropped_streams": self.streams_dropped,
            "avg_recovery_latency_s": round(statistics.fmean(lat), 3) if lat else None,
            "max_recovery_latency_s": round(max(lat), 3) if lat else None,
        }


# --------------------------------------------------------------------------- #
# 1. The spawner
# --------------------------------------------------------------------------- #
async def spawn_tourists(
    count: int = 50,
    api_base: str = API_BASE,
    waitlist_probability: float = 0.85,
) -> List[str]:
    """Fire `count` concurrent POST /api/v1/route, return the workflow ids.

    The demo payload is used so every tourist plans the same Tamil Nadu circuit
    and therefore shares leg ids -- which is what lets the saboteur degrade a
    *known* leg. A high waitlist_probability keeps legs pending so a disruption
    still has something to unwind.
    """
    body = {"use_demo_data": True, "waitlist_probability": waitlist_probability}
    timeout = httpx.Timeout(120.0, connect=15.0)

    async with httpx.AsyncClient(base_url=api_base, timeout=timeout) as client:
        async def one() -> Optional[str]:
            try:
                resp = await client.post("/api/v1/route", json=body)
                resp.raise_for_status()
                return resp.json().get("workflow_id")
            except Exception as exc:  # noqa: BLE001 - one tourist failing is data
                print(f"  [spawn] failed: {type(exc).__name__}: {exc}")
                return None

        results = await asyncio.gather(*[one() for _ in range(count)])

    return [wf for wf in results if wf]


# --------------------------------------------------------------------------- #
# 3. The observer (one task per workflow, open until the run ends)
# --------------------------------------------------------------------------- #
async def observe_stream(
    workflow_id: str,
    ledger: Ledger,
    api_base: str = API_BASE,
    stop: Optional[asyncio.Event] = None,
) -> None:
    """Stream one workflow's SSE feed into the ledger until `stop` is set."""
    stop = stop or asyncio.Event()
    url = f"{api_base}/api/v1/route/{workflow_id}/stream?heartbeat_seconds=2"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(90.0)) as client:
            async with aconnect_sse(client, "GET", url) as event_source:
                async for sse in event_source.aiter_sse():
                    if sse.event == "ready":
                        await ledger.mark_ready(workflow_id)
                        continue
                    if not sse.data or sse.data.startswith(":"):
                        continue
                    try:
                        event = json.loads(sse.data)
                    except json.JSONDecodeError:
                        continue
                    await ledger.observe(workflow_id, event)
                    if event.get("status") in ("fallback_secured", "fallback_failed"):
                        return  # terminal for this workflow; close the socket
                    if stop.is_set():
                        return
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - a dropped socket is a metric
        print(f"  [sse] {workflow_id[:24]} dropped: {type(exc).__name__}")
        await ledger.mark_dropped(workflow_id)


# --------------------------------------------------------------------------- #
# 2. The saboteur
# --------------------------------------------------------------------------- #
async def inject_chaos(
    workflow_ids: Sequence[str],
    sabotage_rate: float = 0.3,
    bootstrap: str = KAFKA_BOOTSTRAP,
    topic: str = TRAFFIC_TOPIC,
    ledger: Optional[Ledger] = None,
    leg_id: Optional[str] = None,
    seed: Optional[int] = None,
) -> List[str]:
    """Blast `traffic_updates` for a random fraction of the live workflows.

    The message mirrors exactly what app/ingestion/stream.py forwards to the
    workflow, so the real Kafka bridge is exercised rather than bypassed.

    `leg_id` may be passed explicitly; otherwise each workflow's real plan is
    read and its first TRAIN leg is degraded. Hardcoding a leg id is wrong --
    the solver names edges `e_<from>_<to>`, and a signal naming a leg the
    itinerary does not contain is silently dropped by the workflow.
    """
    if not workflow_ids:
        return []
    rng = random.Random(seed)
    k = max(1, int(round(len(workflow_ids) * sabotage_rate)))
    targets = rng.sample(list(workflow_ids), k)

    if leg_id is None:
        legs = await _first_leg_per_workflow(targets, api_base=API_BASE)
        targets = [wf for wf in targets if legs.get(wf)]
        leg_ids = [legs[wf] for wf in targets]
    else:
        leg_ids = [leg_id] * len(targets)

    if not targets:
        return []

    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for wf, leg in zip(targets, leg_ids):
            payload = {
                "workflow_id": wf,
                "leg_id": leg,
                "p_confirm": SABOTAGE_P_CONFIRM,
                "delay_minutes": 0,
            }
            await producer.send_and_wait(topic, json.dumps(payload).encode("utf-8"))
            if ledger is not None:
                await ledger.mark_sabotaged(wf)
    finally:
        await producer.stop()
    return targets


async def _first_leg_per_workflow(
    workflow_ids: Sequence[str],
    api_base: str = API_BASE,
    timeout: float = 45.0,
) -> Dict[str, str]:
    """Map workflow_id -> the id of a TRAIN leg in its live plan.

    A TRAIN leg is preferred because only a waitlisted train can be unwound
    into a cab; a BUS leg has no confirmation to degrade.

    Polls, because the workflow only populates ``ctx.itinerary`` *after* the
    Ray solve returns (the plan arrives with the solution). Querying sooner
    returns an empty leg list, and a signal naming no real leg is dropped.
    """
    out: Dict[str, str] = {}
    pending = list(workflow_ids)
    deadline = time.monotonic() + timeout
    http_timeout = httpx.Timeout(20.0, connect=8.0)

    while pending and time.monotonic() < deadline:
        async with httpx.AsyncClient(base_url=api_base, timeout=http_timeout) as client:

            async def one(wf: str) -> None:
                try:
                    r = await client.get(f"/api/v1/itinerary/{wf}")
                    r.raise_for_status()
                    legs = r.json().get("legs") or []
                except Exception:  # noqa: BLE001 - retry until the deadline
                    return
                trains = [lg for lg in legs if lg.get("mode") == "TRAIN"]
                chosen = trains or legs
                if chosen:
                    out[wf] = str(
                        chosen[0].get("leg_id") or chosen[0].get("train_id") or ""
                    )

            await asyncio.gather(*[one(wf) for wf in pending])

        pending = [wf for wf in pending if wf not in out]
        if pending:
            await asyncio.sleep(1.5)

    return {k: v for k, v in out.items() if v}


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
async def run_chaos(
    tourists: int = 50,
    sabotage_rate: float = 0.3,
    api_base: str = API_BASE,
    timeout: float = 60.0,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    ledger = Ledger()
    stop = asyncio.Event()
    started = time.monotonic()

    print(f"==> spawning {tourists} tourists against {api_base}")
    workflow_ids = await spawn_tourists(tourists, api_base=api_base)
    ledger.total = len(workflow_ids)
    print(f"    {ledger.total} workflows started")

    observers = [
        asyncio.create_task(observe_stream(wf, ledger, api_base, stop))
        for wf in workflow_ids
    ]

    # Wait until every SSE subscription is actually registered before sabotaging.
    # Redis Pub/Sub silently drops an update that has zero subscribers, so a
    # fixed sleep is not enough: under load the streams take a variable time to
    # connect and the first disruptions would be lost. `_ready` fires once the
    # 'ready' frame has been seen on every stream.
    ready = asyncio.Event()

    async def _await_ready() -> None:
        try:
            await asyncio.wait_for(ready.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            log_msg = "SSE subscriptions not all ready; sabotaging anyway"
            print(f"    {log_msg}")

    ready_task = asyncio.create_task(_await_ready())
    # `observe_stream` sets this via the ledger's ready callback.
    ledger.on_ready = lambda wf: (
        ready.set() if len(ledger.ready_streams) >= len(workflow_ids) else None
    )
    await ready_task

    print(f"==> {len(workflow_ids)} SSE streams subscribed")

    print(f"==> sabotaging ~{sabotage_rate:.0%} of workflows via Kafka")
    try:
        targets = await inject_chaos(
            workflow_ids, sabotage_rate=sabotage_rate, ledger=ledger, seed=seed
        )
        print(f"    {len(targets)} legs degraded to p_confirm={SABOTAGE_P_CONFIRM}")
    except Exception as exc:  # noqa: BLE001 - report, don't crash the run
        print(f"    Kafka sabotage failed: {type(exc).__name__}: {exc}")

    print(f"==> waiting up to {timeout:.0f}s for recovery")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and ledger.pending():
        await asyncio.sleep(0.5)

    stop.set()
    for task in observers:
        task.cancel()
    await asyncio.gather(*observers, return_exceptions=True)

    report = await ledger.report()
    report["elapsed_s"] = round(time.monotonic() - started, 2)
    report["timed_out"] = bool(ledger.pending())
    return report


def _print_report(report: Dict[str, Any]) -> None:
    print("\n" + "=" * 58)
    print("  PAYANAM CHAOS REPORT")
    print("=" * 58)
    for key, label in [
        ("total_trips", "Total Trips"),
        ("sabotaged_trips", "Sabotaged Trips"),
        ("disrupted_trips", "Disrupted (re-routing seen)"),
        ("recovered_trips", "Recovered (fallback_secured)"),
        ("failed_trips", "Failed Trips"),
        ("dropped_streams", "Dropped SSE Streams"),
    ]:
        print(f"  {label:<34} {report.get(key)}")
    lat = report.get("avg_recovery_latency_s")
    print(f"  {'Avg Recovery Latency (s)':<34} {lat if lat is not None else 'n/a'}")
    print(f"  {'Max Recovery Latency (s)':<34} {report.get('max_recovery_latency_s')}")
    print(f"  {'Elapsed (s)':<34} {report.get('elapsed_s')}")
    if report.get("timed_out"):
        print("  !! timed out with workflows still unrecovered")
    print("=" * 58)


def main() -> int:
    parser = argparse.ArgumentParser(description="Payanam chaos monkey")
    parser.add_argument("--tourists", type=int, default=50)
    parser.add_argument("--sabotage-rate", type=float, default=0.3)
    parser.add_argument("--api", default=API_BASE)
    parser.add_argument("--kafka", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    report = asyncio.run(
        run_chaos(
            tourists=args.tourists,
            sabotage_rate=args.sabotage_rate,
            api_base=args.api,
            timeout=args.timeout,
            seed=args.seed,
        )
    )
    _print_report(report)
    # Exit 0 unless nothing was sabotaged, i.e. the run proved nothing.
    return 0 if report["sabotaged_trips"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
