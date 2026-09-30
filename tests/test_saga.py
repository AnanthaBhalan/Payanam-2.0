"""Saga compensation tests using Temporal's in-process test environment.

Run with:  python -m tests.test_saga

These assert the *ordering* contract, which is the whole point of the Saga:
compensations are registered before the forward step, unwound in reverse, and
the cab fallback only runs after a clean unwind.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# The sandboxed runner is exercised separately (payanam_SANDBOX=1); the
# default run uses the unsandboxed runner so the Saga *logic* is under test.
USE_SANDBOX = os.getenv("payanam_SANDBOX", "0") == "1"

logging.disable(logging.CRITICAL)

from temporalio import activity  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from temporalio.worker.workflow_sandbox import (  # noqa: E402
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)
from temporalio.worker.workflow_sandbox._runner import (  # noqa: E402
    UnsandboxedWorkflowRunner,
)

from app.workflows.activities import (  # noqa: E402
    book_cab,
    book_train,
    cancel_train,
    publish_booking_event,
)
from app.workflows.itinerary import ItineraryWorkflow  # noqa: E402

# book_train and cancel_train must not be passed to the worker: the tests
# replace them with instrumented stand-ins that record call order.
ORDER: list[str] = []


@activity.defn(name="book_train")
async def fake_book_train(
    itinerary_id: str,
    train_id: str,
    origin: str,
    destination: str,
    travel_time_min: int,
    waitlist_probability: float = 0.0,
) -> dict:
    ORDER.append(f"book_train:{train_id}")
    # waitlist_probability is per-leg, so a single leg can drop in isolation.
    if waitlist_probability >= 1.0:
        from app.workflows.activities import WaitlistDroppedError

        raise WaitlistDroppedError(train_id, itinerary_id)  # non-retryable
    return {
        "booking_id": f"trn-{train_id}",
        "itinerary_id": itinerary_id,
        "kind": "TRAIN",
        "reference": f"REF-{train_id}",
        "amount_inr": 100.0,
        "created_at": "2026-01-01T00:00:00",
    }


@activity.defn(name="cancel_train")
async def fake_cancel_train(itinerary_id: str, train_id: str) -> dict:
    ORDER.append(f"cancel_train:{train_id}")
    return {"cancelled": True, "booking_id": f"trn-{train_id}", "refund_inr": 100.0}


@activity.defn(name="book_cab")
async def fake_book_cab(
    itinerary_id: str, origin: str, destination: str, reason: str = "x"
) -> dict:
    ORDER.append("book_cab")
    return {
        "booking_id": "cab-1",
        "itinerary_id": itinerary_id,
        "kind": "CAB",
        "reference": "CAB-REF",
        "amount_inr": 2400.0,
        "created_at": "2026-01-01T00:00:00",
    }


@activity.defn(name="publish_booking_event")
async def fake_publish(event: dict) -> dict:
    return {"published": True}


def _legs() -> list[dict]:
    return [
        {"train_id": "T1", "origin": "CHN", "destination": "TAN", "travel_time_min": 180},
        {"train_id": "T2", "origin": "TAN", "destination": "KUM", "travel_time_min": 90},
    ]


def _collapse(entries: list[str]) -> list[str]:
    """Drop consecutive duplicates.

    A retried activity produces repeated entries; the Saga *ordering* contract
    is about the sequence of distinct operations, so collapse runs first.
    """
    out: list[str] = []
    for item in entries:
        if not out or out[-1] != item:
            out.append(item)
    return out


async def _run(legs, waitlist_probability: float) -> dict:
    # The workflow is sandboxed, but our own first-party modules are passed
    # through: they are code we control and perform stdlib introspection at
    # import time that the default restrictions reject.
    restrictions = SandboxRestrictions.default.with_passthrough_modules(
        "app.workflows", "app", "dataclasses"
    )
    workflow_runner = SandboxedWorkflowRunner(restrictions=restrictions) if USE_SANDBOX else UnsandboxedWorkflowRunner()

    try:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            worker = Worker(
                env.client,
                task_queue="test-saga",
                workflows=[ItineraryWorkflow],
                activities=[
                    fake_book_train, fake_cancel_train,
                    fake_book_cab, fake_publish,
                ],
                workflow_runner=workflow_runner,
            )
            async with worker:
                handle = await env.client.start_workflow(
                    ItineraryWorkflow.run,
                    args=["itin-1", legs, None, waitlist_probability],
                    id=f"saga-{id(legs)}-{len(ORDER)}",
                    task_queue="test-saga",
                )
                return await handle.result()
    except Exception as exc:  # noqa: BLE001 - surface the real failure
        print("WORKFLOW ERROR:", type(exc).__name__, str(exc)[:300])
        raise


async def test_happy_path_no_compensation() -> None:
    ORDER.clear()
    result = await _run(_legs(), 0.0)
    assert result is not None, "workflow returned None"
    assert result["state"] == "COMPLETED", result
    assert result["compensations_run"] == [], result
    assert result["fallback_used"] is False
    assert len(result["booked"]) == 2
    assert ORDER == ["book_train:T1", "book_train:T2"], ORDER
    print("PASS  test_happy_path_no_compensation")


# Bookings must be attempted exactly once even though the retry policy allows
# three attempts: the WaitlistDroppedError is non-retryable, so a drop is not
# retried. This is asserted separately.
async def test_waitlist_drop_compensates_then_falls_back() -> None:
    ORDER.clear()
    legs = _legs()
    result = await _run(legs, 1.0)  # every waitlist drops
    assert result["state"] == "FALLBACK_COMPLETED", result
    assert result["fallback_used"] is True, result

    # The first leg's compensation was on the stack when it dropped.
    assert result["compensations_run"] == ["cancel_train"], result
    # The cab fallback is booked only AFTER the unwind completes.
    assert _collapse(ORDER) == ["book_train:T1", "cancel_train:T1", "book_cab"], ORDER
    # Non-retryable: a drop must not burn the retry budget.
    assert ORDER.count("book_train:T1") == 1, ORDER
    assert any(b["kind"] == "CAB" for b in result["booked"])
    print("PASS  test_waitlist_drop_compensates_then_falls_back")


async def test_second_leg_drop_unwinds_both() -> None:
    """A drop on the *second* leg must unwind both compensations, in LIFO order.

    This is the ordering contract: T1 is booked, T2 drops, so the Saga undoes
    T2 first and then T1 -- strictly reverse of registration.
    """
    ORDER.clear()
    legs = _legs()
    legs[0]["waitlist_probability"] = 0.0  # T1 books fine
    legs[1]["waitlist_probability"] = 1.0  # T2 is dropped

    result = await _run(legs, 0.0)

    assert result["state"] == "FALLBACK_COMPLETED", result
    assert result["fallback_used"] is True
    # Both compensations ran, and the *call order* proves the LIFO unwind.
    assert result["compensations_run"] == ["cancel_train", "cancel_train"], result
    assert _collapse(ORDER) == [
        "book_train:T1",
        "book_train:T2",
        "cancel_train:T2",   # most recent registration undone first
        "cancel_train:T1",   # then the earlier one
        "book_cab",
    ], ORDER
    print("PASS  test_second_leg_drop_unwinds_both")


async def main() -> int:
    tests = [
        test_happy_path_no_compensation,
        test_waitlist_drop_compensates_then_falls_back,
        test_second_leg_drop_unwinds_both,
    ]
    failures = 0
    for fn in tests:
        try:
            await fn()
        except AssertionError as exc:
            failures += 1
            print(f"FAIL  {fn.__name__}: {exc}")
    print("\n" + ("ALL SAGA TESTS PASSED" if not failures else f"{failures} FAILURE(S)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
