"""Temporal Activities for the itinerary Saga.

Booking activities are written as **idempotent** operations keyed by
``itinerary_id`` so a retry (or a compensation landing after a partial success)
converges to the same state.

Saga legs:

* ``book_train``   -- forward step, may raise ``WaitlistDroppedError``
* ``cancel_train`` -- the compensation registered *before* ``book_train``
* ``book_cab``     -- the fallback executed after compensation completes

Deterministic failure injection (``waitlist_probability``) lets the
compensation path be exercised without stubbing the whole client.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from temporalio import activity
from temporalio.exceptions import ApplicationError

log = logging.getLogger("payanam.activities")

# Process-local idempotency store. Production would use Redis/Postgres keyed by
# (itinerary_id, booking_id); a dict keeps the sandbox self-contained while
# still exercising the idempotency contract.
_BOOKINGS: Dict[str, Dict[str, Any]] = {}


class WaitlistDroppedError(ApplicationError):
    """A waitlisted rail reservation was not granted.

    Declared **non-retryable**: retrying the same seat on the same train is
    pointless, so the workflow must compensate instead of burning the budget.
    """

    def __init__(self, train_id: str, itinerary_id: str) -> None:
        super().__init__(
            f"waitlist dropped for train {train_id} (itinerary {itinerary_id})",
            non_retryable=True,
        )
        self.train_id = train_id
        self.itinerary_id = itinerary_id


class BookingConflictError(ApplicationError):
    """A hard failure that aborts the Saga without a fallback."""


@dataclass
class BookingReceipt:
    booking_id: str
    itinerary_id: str
    kind: str  # TRAIN | CAB
    reference: str
    amount_inr: float
    created_at: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "booking_id": self.booking_id,
            "itinerary_id": self.itinerary_id,
            "kind": self.kind,
            "reference": self.reference,
            "amount_inr": self.amount_inr,
            "created_at": self.created_at,
        }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _activity_now_iso() -> str:
    """Timestamp for a booking receipt.

    Uses Temporal's deterministic clock when running inside an activity and
    falls back to the wall clock otherwise, so the activity body stays directly
    callable from tests without a worker context.
    """
    try:
        return activity.now().isoformat()
    except (AttributeError, RuntimeError):
        return datetime.now(timezone.utc).isoformat()


def _deterministic_id(prefix: str, *parts: str) -> str:
    """Stable id from the business key -> idempotency across retries."""
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def _should_drop(train_id: str, itinerary_id: str, waitlist_probability: float) -> bool:
    """Deterministic (retry-stable) waitlist simulation.

    Hashes the business key rather than using ``random`` so an activity retry
    sees the same outcome and tests stay reproducible.
    """
    if waitlist_probability <= 0:
        return False
    if waitlist_probability >= 1:
        return True
    digest = hashlib.sha256(f"{train_id}:{itinerary_id}".encode("utf-8")).hexdigest()
    return (int(digest[:8], 16) / 0xFFFFFFFF) < waitlist_probability


def _store(receipt: BookingReceipt) -> Dict[str, Any]:
    payload = receipt.to_dict()
    _BOOKINGS[receipt.booking_id] = payload
    return payload




# --------------------------------------------------------------------------- #
# Activities
# --------------------------------------------------------------------------- #
@activity.defn
async def book_train(
    itinerary_id: str,
    train_id: str,
    origin: str,
    destination: str,
    travel_time_min: int,
    waitlist_probability: float = 0.0,
) -> Dict[str, Any]:
    """Reserve a (possibly waitlisted) rail seat.

    Idempotent: a repeated call with the same ``(itinerary_id, train_id)``
    returns the original receipt instead of double-booking.
    """
    activity.logger.info("book_train %s %s->%s", train_id, origin, destination)
    booking_id = _deterministic_id("trn", itinerary_id, train_id)

    existing = _lookup(booking_id)
    if existing is not None:
        activity.logger.info("book_train idempotent hit for %s", booking_id)
        return {**existing, "idempotent_replay": True}

    if _should_drop(train_id, itinerary_id, waitlist_probability):
        activity.logger.warning(
            "waitlist dropped: train=%s itinerary=%s p=%.2f",
            train_id, itinerary_id, waitlist_probability,
        )
        # non_retryable -> the workflow must run its compensations.
        raise WaitlistDroppedError(train_id, itinerary_id)

    await asyncio.sleep(0.05)  # simulated rail-partner I/O

    receipt = BookingReceipt(
        booking_id=booking_id,
        itinerary_id=itinerary_id,
        kind="TRAIN",
        reference=f"IRCTC-{uuid.uuid5(uuid.NAMESPACE_URL, booking_id).hex[:10].upper()}",
        amount_inr=round(0.85 * travel_time_min, 2),
        created_at=_activity_now_iso(),
    )
    return _store(receipt)


@activity.defn
async def cancel_train(itinerary_id: str, train_id: str) -> Dict[str, Any]:
    """Compensation for :func:`book_train`.

    Safe to call even when the booking never succeeded -- the Saga registers
    this *before* the forward step -- so it no-ops when nothing is on record.
    """
    booking_id = _deterministic_id("trn", itinerary_id, train_id)
    activity.logger.info("cancel_train %s (booking=%s)", train_id, booking_id)

    existing = _lookup(booking_id)
    if existing is None:
        activity.logger.info("cancel_train no-op: nothing booked for %s", booking_id)
        return {"cancelled": False, "booking_id": booking_id, "reason": "no_booking"}

    await asyncio.sleep(0.02)
    _BOOKINGS.pop(booking_id, None)
    refund = float(existing.get("amount_inr") or 0.0)
    activity.logger.info("cancel_train refunded INR %.2f", refund)
    return {"cancelled": True, "booking_id": booking_id, "refund_inr": refund}


@activity.defn
async def publish_itinerary_update(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Publish a workflow state change to Redis Pub/Sub for SSE clients.

    This is an *activity*, not inline workflow code, because the Temporal
    sandbox forbids direct socket/Redis access from workflow code -- the same
    constraint that forced the Ray Job REST handoff.
    """
    import json as _json

    import redis.asyncio as aioredis
    from temporalio.exceptions import ApplicationError

    from ..config import get_settings

    settings = get_settings()
    workflow_id = str(payload.get("workflow_id") or "")
    if not workflow_id:
        raise ApplicationError("MISSING_WORKFLOW_ID", non_retryable=True)

    event = {k: v for k, v in payload.items() if k != "workflow_id"}
    channel = f"payanam:updates:{workflow_id}"

    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        receivers = await client.publish(channel, _json.dumps(event, default=str))
    finally:
        await client.aclose()

    activity.logger.info("published to %s (%d subscriber(s))", channel, receivers)
    return {"channel": channel, "receivers": int(receivers), "event": event}


@activity.defn
async def book_cab(
    itinerary_id: str,
    origin: str,
    destination: str,
    reason: str = "waitlist_dropped",
    radius_km: float = 15.0,
) -> Dict[str, Any]:
    """Fallback leg: substitute a cab for the failed waitlisted train.

    Phase 3 replaces the stub booking with a real dispatch: the failed transit
    hub is resolved to coordinates, the nearest *free* driver is located via
    Redis ``GEOSEARCH``, and that driver is atomically claimed so two recourse
    workflows falling back at once cannot double-book one cab.

    Raises a non-retryable ``NO_FLEET_AVAILABLE`` application error when no cab
    is dispatchable within ``radius_km``.
    """
    booking_id = _deterministic_id("cab", itinerary_id, origin, destination, reason)
    activity.logger.info(
        "book_cab fallback %s->%s (reason=%s)", origin, destination, reason
    )

    existing = _lookup(booking_id)
    if existing is not None:
        return {**existing, "idempotent_replay": True}

    # --- translate the transit hub into a real pickup point --------------
    from ..graph.seed_tn import hub_coords

    try:
        lon, lat = hub_coords(origin)
    except KeyError as exc:
        raise ApplicationError(
            f"NO_PICKUP_COORDINATES: cannot geolocate hub {origin!r}",
            non_retryable=True,
        ) from exc

    # --- geolocate the tourist and claim the nearest free cab ------------
    from ..fleet.matcher import NoFleetAvailableError, dispatch_cab
    from ..fleet.state import get_fleet_state

    try:
        assignment = await dispatch_cab(
            pickup_lon=lon,
            pickup_lat=lat,
            tourist_id=itinerary_id,
            radius_km=radius_km,
            state=get_fleet_state(),
        )
    except NoFleetAvailableError as exc:
        # Terminal: there is no recourse left, so the workflow must stop
        # rather than burn retries on a search that cannot succeed.
        raise ApplicationError(
            f"NO_FLEET_AVAILABLE: {exc}", non_retryable=True
        ) from exc

    receipt = BookingReceipt(
        booking_id=booking_id,
        itinerary_id=itinerary_id,
        kind="CAB",
        reference=f"CAB-{assignment['dispatch_id'][:10].upper()}",
        amount_inr=_FARE_BY_DISTANCE(assignment.get("distance_km", 0.0)),
        created_at=_activity_now_iso(),
    )
    payload = _store(receipt)
    payload["reason"] = reason
    payload["driver_id"] = assignment["driver_id"]
    payload["driver_distance_km"] = assignment.get("distance_km")
    payload["pickup_lon"] = lon
    payload["pickup_lat"] = lat
    payload["dispatch_id"] = assignment["dispatch_id"]
    return payload


def _lookup(booking_id: str) -> Optional[Dict[str, Any]]:
    return _BOOKINGS.get(booking_id)


def _FARE_BY_DISTANCE(km: float) -> float:
    """Base fare plus a per-km rate, floored at the minimum cab fare."""
    return round(max(150.0, 150.0 + 28.0 * float(km)), 2)



@activity.defn
async def book_bus(
    itinerary_id: str,
    origin: str,
    destination: str,
    reason: str = "waitlist_dropped",
) -> Dict[str, Any]:
    """Fallback leg: a confirmed bus seat when the rail option degrades.

    Mirrors :func:`book_cab` and is idempotent on the same business key.
    """
    booking_id = _deterministic_id("bus", itinerary_id, origin, destination, reason)
    activity.logger.info(
        "book_bus fallback %s->%s (reason=%s)", origin, destination, reason
    )

    existing = _lookup(booking_id)
    if existing is not None:
        return {**existing, "idempotent_replay": True}

    await asyncio.sleep(0.05)
    receipt = BookingReceipt(
        booking_id=booking_id,
        itinerary_id=itinerary_id,
        kind="BUS",
        reference=f"SETC-{uuid.uuid5(uuid.NAMESPACE_URL, booking_id).hex[:8].upper()}",
        amount_inr=950.0,
        created_at=_activity_now_iso(),
    )
    payload = _store(receipt)
    payload["reason"] = reason
    return payload


@activity.defn
async def solve_itinerary(
    request_payload: Dict[str, Any],
    time_limit_seconds: float = 5.0,
) -> Dict[str, Any]:
    """Run the CP-SAT model on the Ray cluster and return the solution dict.

    Uses the Ray Job **REST API**, not Ray Client. Ray Client binds its session
    to the calling thread and is not task/thread safe, so every submission
    from a Temporal activity failed with::

        AttributeError: 'InProgressSentinel' object has no attribute 'id'

    Here Temporal POSTs a job to the dashboard, the cluster runs
    ``app/solver/job_entrypoint.py`` which writes the itinerary to Redis, and
    this activity polls for that key. No ray import happens in the worker.

    Raises ``ApplicationError`` if submission fails, the job fails, or the
    budget is exhausted -- the workflow's Saga then handles it.
    """
    import uuid

    from ..fleet.state import get_fleet_state
    from ..solver.ray_dispatch import (
        submit_solve_job,
        wait_for_solve_result,
    )

    task_id = uuid.uuid4().hex
    activity.logger.info("submitting ray solve job task_id=%s", task_id)

    fleet = get_fleet_state()
    redis_client = fleet.redis

    try:
        job_id = await submit_solve_job(
            request_payload,
            task_id=task_id,
            time_limit_seconds=time_limit_seconds,
        )
    except Exception as exc:  # noqa: BLE001 - surfaced as a terminal error
        raise ApplicationError(
            f"RAY_JOB_SUBMISSION_FAILED: {exc}", non_retryable=True
        ) from exc

    try:
        result = await wait_for_solve_result(
            job_id, task_id, redis_client, overall_timeout=300.0
        )
    except Exception as exc:  # noqa: BLE001
        raise ApplicationError(f"RAY_JOB_FAILED: {exc}") from exc
    finally:
        # Best-effort cleanup of the handoff key.
        try:
            await redis_client.delete(f"payanam:solve_result:{task_id}")
        except Exception:  # noqa: BLE001
            pass

    activity.logger.info(
        "ray solve job %s completed for task_id=%s feasible=%s",
        job_id, task_id, result.get("feasible"),
    )
    return result


@activity.defn
async def load_transit_graph() -> Dict[str, Any]:
    """Read the state-wide graph out of Memgraph, ready for the solver."""
    from ..graph.state import AsyncMemgraphClient

    client = AsyncMemgraphClient()
    try:
        await client.connect()
        nodes = await client.run(
            "MATCH (c:City) RETURN c.id AS id, c.name AS name, "
            "c.node_type AS node_type, c.windows AS windows"
        )
        edges = await client.run(
            "MATCH (a:City)-[r:TRANSIT]->(b:City) "
            "RETURN a.id AS origin, b.id AS destination, r.edge_id AS edge_id, "
            "r.mode AS mode, r.travel_time_min AS travel_time_min, "
            "r.cost_inr AS cost_inr, r.probability AS probability, "
            "r.waitlisted AS waitlisted, r.multiplier AS multiplier"
        )
        return {"nodes": [dict(r) for r in nodes], "edges": [dict(r) for r in edges]}
    finally:
        await client.close()


@activity.defn
async def publish_booking_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Emit a booking event onto Redpanda for downstream reconciliation."""
    try:
        from confluent_kafka import Producer

        from ..config import get_settings

        settings = get_settings()
        producer = Producer({"bootstrap.servers": settings.kafka_brokers, "linger.ms": 50})
        producer.produce(
            settings.kafka_topic_bookings,
            key=str(event.get("booking_id", "")).encode("utf-8"),
            value=json.dumps(event, default=str).encode("utf-8"),
        )
        producer.flush(3.0)
        return {"published": True, "topic": settings.kafka_topic_bookings}
    except Exception as exc:  # noqa: BLE001 - telemetry must never fail a Saga
        log.warning("booking event publish skipped: %s", exc)
        return {"published": False, "reason": str(exc)}


# --------------------------------------------------------------------------- #
# Registration & policy
# --------------------------------------------------------------------------- #
# Bound to a task queue by the worker in app/worker.py.
ACTIVITIES = [
    book_train,
    cancel_train,
    book_cab,
    book_bus,
    solve_itinerary,
    load_transit_graph,
    publish_booking_event,
]

# Transient-failure retry policy for the forward and fallback legs.
RETRY_POLICY = {
    "initial_interval": timedelta(seconds=2),
    "backoff_coefficient": 2.0,
    "maximum_interval": timedelta(seconds=30),
    "maximum_attempts": 3,
}

# Compensations are best-effort with a tighter budget: a cancellation outage
# must not stall the workflow, but we do retry the refund a few times.
COMPENSATION_RETRY_POLICY = {
    "initial_interval": timedelta(seconds=1),
    "backoff_coefficient": 2.0,
    "maximum_interval": timedelta(seconds=10),
    "maximum_attempts": 5,
}


def describe_activities() -> Dict[str, str]:
    """Introspection helper for /health and the OpenAPI docs."""
    return {a.__name__: a.__qualname__ for a in ACTIVITIES}
