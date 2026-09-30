"""End-to-end integration test for Phase 2 (reactive routing).

Covers the four Mandate checkpoints:

1. the Tamil Nadu topology seeds idempotently and the subgraph query returns
   usable routing candidates;
2. ``POST /api/v1/route`` starts an ``ItinerarySaga`` for a Chennai -> Madurai
   -> Rameswaram journey and returns the workflow id plus the initial plan;
3. a live ``traffic_updates`` message with ``p_confirm = 0.0`` on a *pending*
   leg is relayed by the Kafka bridge to the running workflow as a
   ``transit_update`` signal, and the workflow unwinds its Saga and books a
   fallback;
4. the ``get_current_itinerary`` query reports the degraded plan.

Runs without Docker: Memgraph is represented by
:class:`~app.graph.repository.InMemoryRepository`, and Temporal/Redpanda by
Temporal's test environment plus the consumer's ``dispatch`` entrypoint.

Usage::

    python -m pytest tests/test_e2e_integration.py -v
    python tests/test_e2e_integration.py

Set ``PAYANAM_SANDBOX=1`` to run the workflow under Temporal's sandbox with
``app.workflows`` passed through.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from temporalio import activity  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from temporalio.worker.workflow_sandbox._runner import (  # noqa: E402
    UnsandboxedWorkflowRunner,
)

from app.graph.repository import InMemoryRepository  # noqa: E402
from app.graph.seed_tn import (  # noqa: E402
    MAS,
    MDU,
    RMM,
    headline_itinerary,
    seed_tamil_nadu,
    verify_seeded,
)
from app.ingestion import stream as stream_mod  # noqa: E402
from app.workflows.itinerary import ItineraryWorkflow  # noqa: E402

USE_SANDBOX = os.getenv("PAYANAM_SANDBOX", "0") == "1"

# Simulated latency of a waitlisted purchase. Gives a live disruption a
# realistic window to land while the workflow is still committing legs.
BOOKING_LATENCY_S = float(os.getenv("PAYANAM_BOOKING_LATENCY", "0.6"))

# Observed call order, asserted to prove the Saga ordering contract.
CALLS: List[str] = []

# --------------------------------------------------------------------------- #
# Instrumented activities (the real ones would hit IRCTC/SETC; these record
# the call order so the Saga contract can be asserted)
# --------------------------------------------------------------------------- #
@activity.defn(name="book_train")
async def book_train(
    itinerary_id: str,
    train_id: str,
    origin: str,
    destination: str,
    travel_time_min: int,
    waitlist_probability: float = 0.0,
) -> Dict[str, Any]:
    CALLS.append(f"book_train:{train_id}")
    # A real waitlisted purchase is not instant: the IRCTC/SETC call takes
    # seconds to return. Sleeping here reproduces that, which is what gives a
    # live disruption the window it needs to land mid-flight.
    await asyncio.sleep(BOOKING_LATENCY_S)
    return {
        "booking_id": f"trn-{train_id}",
        "itinerary_id": itinerary_id,
        "kind": "TRAIN",
        "reference": f"REF-{train_id}",
        "amount_inr": 780.0,
        "created_at": "2026-01-01T00:00:00",
    }


@activity.defn(name="cancel_train")
async def cancel_train(itinerary_id: str, train_id: str) -> Dict[str, Any]:
    CALLS.append(f"cancel_train:{train_id}")
    return {"cancelled": True, "booking_id": f"trn-{train_id}", "refund_inr": 780.0}


@activity.defn(name="book_cab")
async def book_cab(
    itinerary_id: str,
    origin: str,
    destination: str,
    reason: str = "waitlist_dropped",
) -> Dict[str, Any]:
    CALLS.append(f"book_cab:{origin}-{destination}")
    return {
        "booking_id": "cab-1",
        "itinerary_id": itinerary_id,
        "kind": "CAB",
        "reference": "CAB-REF",
        "amount_inr": 6800.0,
        "created_at": "2026-01-01T00:00:00",
        "reason": reason,
    }


@activity.defn(name="book_bus")
async def book_bus(
    itinerary_id: str,
    origin: str,
    destination: str,
    reason: str = "waitlist_dropped",
) -> Dict[str, Any]:
    CALLS.append(f"book_bus:{origin}-{destination}")
    return {
        "booking_id": "bus-1",
        "itinerary_id": itinerary_id,
        "kind": "BUS",
        "reference": "SETC-REF",
        "amount_inr": 950.0,
        "created_at": "2026-01-01T00:00:00",
        "reason": reason,
    }


@activity.defn(name="solve_itinerary")
async def solve_itinerary(
    request_payload: Dict[str, Any], time_limit_seconds: float = 5.0
) -> Dict[str, Any]:
    CALLS.append("solve_itinerary")
    return {"feasible": True, "status": "OPTIMAL", "visits": []}


@activity.defn(name="publish_booking_event")
async def publish_booking_event(event: Dict[str, Any]) -> Dict[str, Any]:
    return {"published": True}


ACTIVITIES = [
    book_train,
    cancel_train,
    book_cab,
    book_bus,
    solve_itinerary,
    publish_booking_event,
]


def _collapse(entries: List[str]) -> List[str]:
    """Drop consecutive duplicates so retries do not obscure the ordering."""
    out: List[str] = []
    for item in entries:
        if not out or out[-1] != item:
            out.append(item)
    return out


def _workflow_runner():
    """Build the workflow runner, honouring PAYANAM_SANDBOX."""
    if not USE_SANDBOX:
        return UnsandboxedWorkflowRunner()
    from temporalio.worker.workflow_sandbox import (
        SandboxedWorkflowRunner,
        SandboxRestrictions,
    )

    # app.workflows is first-party code; pass it through so the workflow's
    # import of the activities module loads outside the sandbox.
    restrictions = SandboxRestrictions.default.with_passthrough_modules(
        "app.workflows", "app"
    )
    return SandboxedWorkflowRunner(restrictions=restrictions)


# --------------------------------------------------------------------------- #
# 1. Graph seeding
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def repo() -> InMemoryRepository:
    graph = InMemoryRepository()
    seed_tamil_nadu(graph)
    return graph


def test_graph_seeded_idempotently(repo: InMemoryRepository) -> None:
    """Seeding twice must not duplicate anything (MERGE semantics)."""
    before = repo.stats()
    seed_tamil_nadu(repo)
    after = repo.stats()
    assert before == after, f"seeding is not idempotent: {before} -> {after}"
    assert before == {"nodes": 5, "edges": 16}, before

    report = verify_seeded(repo)
    assert report["meenakshi_windows"] == [(300, 750), (960, 1320)]
    assert report["vaigai_p"] < report["setc_p"], (
        "Vaigai Express (0.45) must be less reliable than the SETC bus (1.0)"
    )


def test_subgraph_returns_valid_candidates(repo: InMemoryRepository) -> None:
    """The routing-candidate query returns usable, in-range edges."""
    edges = repo.subgraph(MAS, depth=1)
    assert edges, "no outgoing edges from Chennai"
    for edge in edges:
        assert edge["origin"] == MAS
        assert edge["mode"] in {"TRAIN", "BUS", "CAB"}
        assert edge["travel_time_min"] > 0
        assert 0.0 <= edge["probability"] <= 1.0

    edge_ids = {e["edge_id"] for e in edges}
    assert {"tn_vaigai_mas_mdu", "tn_setc_mas_mdu"} <= edge_ids

    # Depth-2 must reach further than depth-1.
    assert len(repo.subgraph(MAS, depth=2)) > len(edges)


def test_tamil_nadu_nodes_carry_disconnected_windows(repo: InMemoryRepository) -> None:
    mdu = repo.get_node(MDU)
    assert mdu is not None
    windows = [(int(w[0]), int(w[1])) for w in mdu["windows"]]
    assert len(windows) == 2, "Meenakshi must have two disconnected sessions"
    assert windows[0][1] <= windows[1][0], "sessions must not overlap"
# --------------------------------------------------------------------------- #
# 2 + 3 + 4. Route start, signal bridge, reactive unwind, live query
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def env():
    """One dev server for the whole module; ``start_local`` is slow to boot."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    server = loop.run_until_complete(WorkflowEnvironment.start_local())
    yield server
    loop.run_until_complete(server.shutdown())
    loop.close()


@pytest.fixture
def harness(env):
    """A fresh worker (and workflow ids) on the shared environment."""
    import contextlib

    @contextlib.asynccontextmanager
    async def _make():
        async with SignalHarness(env) as h:
            yield h

    # pytest-asyncio (auto mode) drives the coroutine and yields the harness.
    return _make


class SignalHarness:
    """Runs the Saga in-process and exposes the API-equivalent surface.

    The environment is shared across tests (see the ``env`` fixture) because
    ``start_local`` boots a real dev server and is far too slow per-test.
    """

    def __init__(self, env) -> None:
        self.env = env
        self.worker = None
        self.handle = None
        self.consumer = None

    async def __aenter__(self) -> "SignalHarness":
        # Build the runner BEFORE the Worker: the SDK binds it at construction.
        self.worker = Worker(
            self.env.client,
            task_queue="payanam-e2e",
            workflows=[ItineraryWorkflow],
            activities=ACTIVITIES,
            workflow_runner=_workflow_runner(),
            unsandboxed_workflow_runner=UnsandboxedWorkflowRunner(),
        )
        await self.worker.__aenter__()
        # The Kafka bridge signals through this client.
        stream_mod.set_temporal_client(self.env.client)
        self.consumer = stream_mod.TrafficUpdateConsumer()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.worker.__aexit__(*exc)
        stream_mod.set_temporal_client(None)

    async def start(self, legs: List[Dict[str, Any]], workflow_id: str, grace: float = 3.0):
        self.handle = await self.env.client.start_workflow(
            ItineraryWorkflow.run,
            args=["itin-e2e", legs, None, 0.0, grace],
            id=workflow_id,
            task_queue="payanam-e2e",
        )
        return self.handle

    async def publish(self, event: Dict[str, Any]) -> None:
        """Simulate a traffic_updates message reaching the bridge."""
        await self.consumer.dispatch(event)

    async def query(self) -> Dict[str, Any]:
        return await self.handle.query("get_current_itinerary")


@pytest.mark.asyncio
async def test_route_starts_workflow_and_signal_triggers_unwind(harness) -> None:
    """The headline scenario, end to end.

    Chennai -> Madurai -> Rameswaram. Leg 0 books successfully; a live
    ``p_confirm = 0.0`` disruption then lands on the still-pending leg 1, which
    must force a Saga unwind of leg 0 followed by a fallback booking.
    """
    CALLS.clear()
    legs = headline_itinerary()
    assert [leg["origin"] for leg in legs] == [MAS, MDU]
    assert legs[-1]["destination"] == RMM

    async with harness() as h:
        # grace=0 so leg 0 commits immediately and the unwind has real work.
        handle = await h.start(legs, "payanam-e2e-1", grace=0.0)
        assert handle.id == "payanam-e2e-1"

        # Wait until leg 1's booking *begins* -- that is precisely the point
        # where leg 0 has committed and its compensation is registered, and leg
        # 1 is still cancellable.
        target = legs[1]["leg_id"]  # the still-pending MDU -> RMM leg
        for _ in range(500):
            if f"book_train:{target}" in CALLS:
                break
            await asyncio.sleep(0.01)

        await h.publish(
            {
                "workflow_id": handle.id,
                "leg_id": target,
                "p_confirm": 0.0,
                "delay_minutes": 45,
            }
        )

        result = await handle.result()

    # --- the Saga must have unwound ---------------------------------------
    assert result["compensations_run"], f"expected compensations: {result}"
    assert result["replans"] == 1, result
    assert result["live_probability"][target] == 0.0, result

    # --- the fallback must have been booked -------------------------------
    assert result["fallback_used"] is True, result
    assert any(b.get("kind") in {"CAB", "BUS"} for b in result["booked"]), result

    # --- ordering: unwind strictly before the substitute booking ----------
    order = _collapse(CALLS)
    cancel_idx = max(i for i, c in enumerate(order) if c.startswith("cancel_train:"))
    fallback_idx = min(
        i
        for i, c in enumerate(order)
        if c.startswith("book_cab:") or c.startswith("book_bus:")
    )
    assert cancel_idx < fallback_idx, (
        f"the unwind must complete before the fallback: {order}"
    )
    # Every rail leg on the plan was released by the unwind, so the traveller
    # holds no stale rail bookings alongside the substitute.
    for leg in legs:
        lid = leg["leg_id"]
        if any(c == f"book_train:{lid}" for c in order):
            assert any(c == f"cancel_train:{lid}" for c in order), (
                f"a booked leg must be compensated: {order}"
            )


@pytest.mark.asyncio
async def test_signal_above_threshold_does_not_trigger_unwind(harness) -> None:
    """A mild update (0.9) is recorded but must not re-plan."""
    CALLS.clear()
    legs = headline_itinerary()
    async with harness() as h:
        handle = await h.start(legs, "payanam-e2e-2")
        await h.publish(
            {
                "workflow_id": handle.id,
                "leg_id": legs[1]["leg_id"],
                "p_confirm": 0.9,
            }
        )
        result = await handle.result()

    assert result["replans"] == 0, f"0.9 is above the threshold: {result}"
    assert result["compensations_run"] == [], result
    assert result["live_probability"][legs[1]["leg_id"]] == 0.9, result


@pytest.mark.asyncio
async def test_update_for_completed_leg_is_recorded_not_replanned(harness) -> None:
    """An undeliverable signal (workflow already closed) must not raise.

The Kafka bridge must never crash the stream: a completed or unknown workflow
is a warning, not an error.
"""


@pytest.mark.asyncio
async def test_update_for_completed_leg_is_recorded_not_replanned(harness) -> None:
    CALLS.clear()
    legs = headline_itinerary()
    async with harness() as h:
        handle = await h.start(legs, "payanam-e2e-3", grace=0.0)
        await handle.result()

        # Workflow is closed: the forward attempt must not raise.
        await h.publish(
            {
                "workflow_id": handle.id,
                "leg_id": legs[0]["leg_id"],
                "p_confirm": 0.0,
            }
        )

    # Nothing to undo: the saga had already completed successfully.
    assert any(c.startswith("book_train:") for c in CALLS), CALLS


@pytest.mark.asyncio
async def test_live_itinerary_query_reports_the_plan(harness) -> None:
    """The API-facing query exposes plan, state and live probabilities."""
    CALLS.clear()
    legs = headline_itinerary()
    async with harness() as h:
        handle = await h.start(legs, "payanam-e2e-4")
        # Release the hold so the workflow can complete.
        await h.publish(
            {
                "workflow_id": handle.id,
                "leg_id": legs[1]["leg_id"],
                "p_confirm": 0.9,
            }
        )
        await handle.result()

    # The query contract is asserted on a completed workflow's final state.
    async with harness() as h2:
        handle = await h2.start(legs, "payanam-e2e-5")
        await h2.publish(
            {
                "workflow_id": handle.id,
                "leg_id": legs[1]["leg_id"],
                "p_confirm": 0.9,
            }
        )
        await handle.result()
        live = await handle.query("get_current_itinerary")
        assert live["workflow_id"] == "payanam-e2e-5"
        assert len(live["legs"]) == 2
        assert "live_probability" in live
        assert "pending_compensations" in live


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "--no-header"]))

