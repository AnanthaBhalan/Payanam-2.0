"""Temporal Workflow implementing the itinerary Saga.

Ordering contract (this is the whole point of the file):

1. A compensation is appended to ``compensations`` **BEFORE** the forward
   activity is executed.  If the worker dies mid-``book_train`` the booking may
   have partially committed; the compensation is already on the stack and will
   run, and because ``cancel_train`` is idempotent it is harmless when the
   booking never happened.
2. On failure the compensations are unwound in **reverse registration order**
   (LIFO) -- the standard Saga invariant, so later steps are undone first.
3. A single compensation failure never aborts the unwind; errors are recorded
   and the remaining compensations still run.
4. Only after a clean unwind does the fallback ``book_cab`` execute.

Phase 2 adds *reactivity*.  A running workflow is long-lived, so a plan made at
T+0 can be invalidated by a disruption at T+30s.  The workflow therefore
accepts ``transit_update`` signals carrying a degraded confirmation
probability, and re-plans in-flight:

5. A signal never mutates history; the handler only appends to an in-memory
   update queue.  The main coroutine drains that queue at safe points (which
   keeps replay deterministic).
6. When a queued update degrades a *pending* leg below ``DEGRADED_THRESHOLD``,
   the workflow abandons the remaining bookings, unwinds what it holds, and
   books a substitute -- the same Saga machinery as a booking failure, entered
   from a different trigger.

All workflow code is deterministic: no wall-clock reads, no direct I/O, no
randomness.  Every external touch is an activity call.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    FailureError,
    TemporalError,
)

# The workflow needs the activity *definitions* (name + argument signature) but
# never their bodies -- those run in a worker process. ``activities`` imports
# uuid/hashlib/asyncio and friends, which the sandbox cannot load, so the module
# is passed through. Only the activity defn metadata crosses this boundary.
with workflow.unsafe.imports_passed_through():
    from .activities import (  # noqa: F401
        COMPENSATION_RETRY_POLICY,
        RETRY_POLICY,
        WaitlistDroppedError,
        book_bus,
        book_cab,
        book_train,
        cancel_train,
        publish_booking_event,
        solve_itinerary,
    )

log = logging.getLogger("payanam.workflow")

# Marker used to recognise a dropped waitlist after it crosses the activity
# boundary (the concrete exception type is not preserved by Temporal).
_WAITLIST_MARKER = "waitlist dropped"

# A leg whose live confirmation probability falls below this is treated as
# unbookable: at 15% the expected cost of chasing it exceeds the fallback.
DEGRADED_THRESHOLD = 0.15


def _is_waitlist_drop(exc: BaseException, _depth: int = 0) -> bool:
    """True when ``exc`` (or anything in its cause chain) is a waitlist drop.

    Temporal wraps activity failures, so the marker can sit several levels
    down. The walk is bounded so a self-referential cause cannot hang the
    workflow.
    """
    if _depth > 10:
        return False
    if _WAITLIST_MARKER in str(exc):
        return True
    cause = getattr(exc, "cause", None)
    if isinstance(cause, BaseException):
        return _is_waitlist_drop(cause, _depth + 1)
    return False

# NOTE: the workflow deliberately avoids importing ``app.models`` (pydantic
# touches the filesystem, which the workflow sandbox forbids). Payloads are
# passed as plain dicts and re-validated on the API/activity side, where the
# richer ``RoutingRequest``/``RouteResult`` models live.



# Plain classes, not dataclasses: @dataclass resolves type hints through
# sys.modules at class-creation time, which the workflow sandbox rejects.
class Compensation:
    """A registered undo step plus the bookkeeping the result needs."""

    __slots__ = ("name", "run", "args")

    def __init__(self, name: str, run, args: tuple = ()) -> None:
        self.name = name
        self.run = run
        self.args = args


class SagaContext:
    """Mutable state carried through the Saga (deterministic, in-memory)."""

    def __init__(self, itinerary_id: str) -> None:
        self.itinerary_id = itinerary_id
        self.booked: List[Dict[str, Any]] = []
        self.compensations: List[Compensation] = []
        self.compensations_run: List[str] = []
        self.compensation_errors: List[Dict[str, str]] = []
        self.fallback_used: bool = False
        self.solution: Optional[Dict[str, Any]] = None
        self.state: str = "PENDING"
        self.failure: Optional[str] = None

        # -- phase 2: reactive state -------------------------------------
        # Signals append here; the main coroutine drains it at safe points.
        self.updates: List[Dict[str, Any]] = []
        self.updates_applied: List[Dict[str, Any]] = []
        # leg_id -> live confirmation probability
        self.live_probability: Dict[str, float] = {}
        # the ordered plan, published through the query
        self.itinerary: List[Dict[str, Any]] = []
        self.completed_legs: List[str] = []
        self.replans: int = 0

    # -- reactive bookkeeping --------------------------------------------
    def queue_update(self, update: Dict[str, Any]) -> None:
        """Append a signal payload to the pending-update queue."""
        self.updates.append(update)

    def drain_updates(self) -> List[Dict[str, Any]]:
        """Take everything queued so far and fold it into live state."""
        pending, self.updates = self.updates, []
        for upd in pending:
            leg_id = str(upd.get("leg_id") or "")
            try:
                p = float(upd.get("new_p_confirm", 1.0))
            except (TypeError, ValueError):
                p = 1.0
            self.live_probability[leg_id] = max(0.0, min(1.0, p))
            self.updates_applied.append(upd)
        return pending

    def degraded_legs(
        self, candidates: List[str], threshold: float = DEGRADED_THRESHOLD
    ) -> List[str]:
        """Which of ``candidates`` have been pushed below ``threshold``."""
        return [
            leg_id
            for leg_id in candidates
            if leg_id in self.live_probability
            and self.live_probability[leg_id] < threshold
        ]

    def register(self, name: str, run, *args) -> None:
        """Register a compensation.  MUST be called before the forward step.

        ``name`` is for reporting/history only; ``run`` is the activity
        definition actually dispatched during the unwind.
        """
        self.compensations.append(Compensation(name=name, run=run, args=args))
        workflow.logger.info("registered compensation %s", name)

    async def unwind(self) -> List[str]:
        """Run every registered compensation in reverse (LIFO) order."""
        executed: List[str] = []
        while self.compensations:
            comp = self.compensations.pop()  # reverse order
            try:
                result = await workflow.execute_activity(
                    comp.run,
                    args=list(comp.args),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(**COMPENSATION_RETRY_POLICY),
                )
                executed.append(comp.name)
                self.compensations_run.append(comp.name)
                workflow.logger.info("compensation %s -> %s", comp.name, result)
            except (ActivityError, ApplicationError, FailureError) as exc:
                # One failed undo must not strand the rest of the stack.
                self.compensation_errors.append({"name": comp.name, "error": str(exc)})
                workflow.logger.error("compensation %s failed: %s", comp.name, exc)
        return executed



# --------------------------------------------------------------------------- #
# Workflow
# --------------------------------------------------------------------------- #
@workflow.defn(name="ItinerarySaga")
class ItineraryWorkflow:
    """Plans an itinerary and books it under the Saga contract.

    Input: ``itinerary_id``, ``legs`` (booking legs), ``request_payload``
    (optional CP-SAT payload) and ``waitlist_probability``.
    """

    def __init__(self) -> None:
        self.ctx: Optional[SagaContext] = None

    # ------------------------------------------------------------- queries
    @workflow.query(name="compensation_stack")
    def compensation_stack(self) -> List[str]:
        """Pending compensations (visible live in the Temporal UI)."""
        if self.ctx is None:
            return []
        return [c.name for c in self.ctx.compensations]

    @workflow.query(name="saga_state")
    def saga_state(self) -> str:
        return self.ctx.state if self.ctx else "IDLE"

    @workflow.query(name="get_current_itinerary")
    def get_current_itinerary(self) -> Dict[str, Any]:
        """The live plan, readable by the API while the workflow is running.

        Reports what is booked, what is still pending, and any live probability
        degradations received so far.
        """
        if self.ctx is None:
            return {"state": "IDLE", "legs": [], "booked": []}
        return {
            "workflow_id": workflow.info().workflow_id,
            "state": self.ctx.state,
            "legs": list(self.ctx.itinerary),
            "booked": list(self.ctx.booked),
            "completed_legs": list(self.ctx.completed_legs),
            "live_probability": dict(self.ctx.live_probability),
            "updates_applied": list(self.ctx.updates_applied),
            "compensations_run": list(self.ctx.compensations_run),
            "pending_compensations": [c.name for c in self.ctx.compensations],
            "fallback_used": self.ctx.fallback_used,
            "replans": self.ctx.replans,
            "failure": self.ctx.failure,
        }

    # --------------------------------------------------------------- signals
    @workflow.signal(name="transit_update")
    def transit_update(
        self,
        leg_id: str,
        new_p_confirm: float = 1.0,
        delay_minutes: int = 0,
    ) -> None:
        """Receive a live disruption for one leg.

        The handler is intentionally *pure bookkeeping*: it validates and
        queues the update and returns. All reaction happens in the main
        coroutine at a deterministic point (see ``_apply_pending_updates``),
        because a signal handler must never await an activity itself.
        """
        if self.ctx is None:
            workflow.logger.warning("transit_update before run() started; ignored")
            return
        try:
            p = float(new_p_confirm)
        except (TypeError, ValueError):
            workflow.logger.warning("non-numeric p_confirm in signal; ignored")
            return
        p = max(0.0, min(1.0, p))
        try:
            delay = int(delay_minutes or 0)
        except (TypeError, ValueError):
            delay = 0

        self.ctx.queue_update(
            {
                "leg_id": str(leg_id),
                "new_p_confirm": p,
                "delay_minutes": max(0, delay),
            }
        )
        workflow.logger.info(
            "queued transit_update leg=%s p_confirm=%.3f delay=%smin",
            leg_id, p, delay,
        )

    # ------------------------------------------------------------------ run
    @workflow.run
    async def run(
        self,
        itinerary_id: str,
        legs: List[Dict[str, Any]],
        request_payload: Optional[Dict[str, Any]] = None,
        waitlist_probability: float = 0.0,
        replan_grace_seconds: float = 0.0,
    ) -> Dict[str, Any]:
        ctx = SagaContext(itinerary_id=itinerary_id)
        self.ctx = ctx
        ctx.state = "RUNNING"

        # -- phase 1: solve (pure computation, nothing to compensate) -------
        if request_payload:
            ctx.state = "SOLVING"
            try:
                raw = await workflow.execute_activity(
                    solve_itinerary,
                    args=[request_payload, 5.0],
                    start_to_close_timeout=timedelta(seconds=120),
                    retry_policy=RetryPolicy(**RETRY_POLICY),
                )
                ctx.solution = dict(raw)
                if not ctx.solution.get("feasible"):
                    workflow.logger.warning(
                        "no feasible itinerary (%s)", ctx.solution.get("status")
                    )
            except (ActivityError, ApplicationError, FailureError) as exc:
                # A solver failure is not a booking failure -- degrade to the
                # caller-supplied legs instead of compensating.
                workflow.logger.error("solver activity failed: %s", exc)
                ctx.solution = None

        # -- phase 2: the Saga over booking legs ----------------------------
        # Publish the legs up front so the query reports a real plan and a
        # signal arriving mid-flight has something to target.
        ctx.itinerary = [dict(leg) for leg in legs]
        for leg in legs:
            lid = str(leg.get("leg_id") or leg.get("train_id") or "")
            if not lid:
                continue
            # Seed the live confirmation probability from an explicit ``p_confirm``
            # only. ``waitlist_probability`` is the *drop risk* applied at
            # booking time and must NOT seed live state: a bus leg declared with
            # waitlist_probability 0.0 means "never dropped", not "already dead".
            ctx.live_probability.setdefault(
                lid, float(leg.get("p_confirm", 1.0))
            )

        ctx.state = "BOOKING"
        degraded_reason: Optional[str] = None
        try:
            for index, leg in enumerate(legs):
                # Re-planning window: before committing the first booking, give
                # live operators a brief chance to amend the plan. This is what
                # makes a running workflow genuinely *reactive* rather than a
                # straight line. Runs BEFORE the drain check so a signal that
                # lands during the window is still seen.
                if replan_grace_seconds > 0 and index == 0:
                    ctx.state = "AWAITING_UPDATE"
                    deadline = workflow.now() + timedelta(
                        seconds=replan_grace_seconds
                    )
                    while workflow.now() < deadline and not self.ctx.updates:
                        await asyncio.sleep(0.05)
                    ctx.state = "BOOKING"

                # Deterministic safe point: fold in signals that arrived since
                # the previous leg before committing to the next booking.
                if self._apply_pending_updates(ctx, legs[index:]):
                    degraded_reason = (
                        "live transit update dropped a pending leg below "
                        f"{DEGRADED_THRESHOLD:.2f}"
                    )
                    break

                await self._book_one_leg(ctx, leg, waitlist_probability)
                lid = str(leg.get("leg_id") or leg.get("train_id") or "")
                if lid:
                    ctx.completed_legs.append(lid)

                # Re-check after the booking: a disruption can arrive *while*
                # the partner API is in flight, and cancelling a just-made hold
                # is precisely what the Saga compensation stack is for. The
                # whole plan is re-evaluated here because the degraded leg may
                # be the one that just committed.
                if self._apply_pending_updates(ctx, legs):
                    degraded_reason = (
                        "live transit update arrived mid-booking for a leg at "
                        f"or below {DEGRADED_THRESHOLD:.2f}"
                    )
                    break
            ctx.state = "COMPLETED" if not degraded_reason else "REPLAN_PENDING"
        except WaitlistDroppedError as exc:
            # Mandated path: unwind, then fall back to a cab.
            workflow.logger.warning("waitlist dropped, compensating: %s", exc)
            degraded_reason = str(exc)
            ctx.state = "COMPENSATING"
        except (ActivityError, ApplicationError, FailureError) as exc:
            # Unexpected hard failure: unwind, but no fallback.
            workflow.logger.error("leg failed hard, compensating: %s", exc)
            ctx.state = "COMPENSATING"
            degraded_reason = None
            ctx.failure = f"{type(exc).__name__}: {exc}"

        # One unwind + fallback path, two triggers: a booking failure, or a
        # live signal that invalidated a still-pending leg.
        if degraded_reason is not None:
            ctx.replans += 1
            ctx.state = "COMPENSATING"
            await ctx.unwind()
            await self._handle_degradation(ctx, legs, degraded_reason)
        elif ctx.failure is not None:
            await ctx.unwind()
            ctx.state = "FAILED"

        # -- phase 3: best-effort telemetry (never fails the Saga) -----------
        await self._publish_events(ctx)

        # Plain dict: the workflow must not depend on pydantic (see note above).
        return {
            "workflow_id": workflow.info().workflow_id,
            "state": ctx.state,
            "booked": ctx.booked,
            "compensations_run": ctx.compensations_run,
            "compensation_errors": ctx.compensation_errors,
            "failure": ctx.failure,
            "fallback_used": ctx.fallback_used,
            "solution": ctx.solution,
            # -- phase 2 reactive reporting ---------------------------------
            "replans": ctx.replans,
            "updates_applied": ctx.updates_applied,
            "live_probability": dict(ctx.live_probability),
            "completed_legs": ctx.completed_legs,
            "itinerary": ctx.itinerary,
        }

    # -------------------------------------------------------------- reactivity
    def _apply_pending_updates(
        self, ctx: SagaContext, pending_legs: List[Dict[str, Any]]
    ) -> bool:
        """Drain queued signals; return True if a *pending* leg is degraded.

        ``pending_legs`` is the slice of the plan not yet booked. A degradation
        on an already-completed leg is recorded but does not trigger a re-plan:
        that booking is already paid for and is undone via compensation only if
        the caller explicitly asks.
        """
        applied = ctx.drain_updates()
        if not applied:
            return False

        candidates = [
            str(leg.get("leg_id") or leg.get("train_id") or "")
            for leg in pending_legs
        ]
        degraded = ctx.degraded_legs([c for c in candidates if c])
        if not degraded:
            # Updates for completed/unknown legs are recorded, not acted on.
            for upd in applied:
                workflow.logger.info(
                    "transit update applied (no pending impact): %s", upd
                )
            return False

        for leg_id in degraded:
            workflow.logger.warning(
                "probability degradation: leg=%s p_confirm=%.3f < %.2f",
                leg_id,
                ctx.live_probability.get(leg_id, 1.0),
                DEGRADED_THRESHOLD,
            )
        return True

    async def _handle_degradation(
        self, ctx: SagaContext, legs: List[Dict[str, Any]], reason: str
    ) -> None:
        """Unwound already; now book a substitute for the affected leg(s).

        The Saga guarantee is what makes this safe: every completed booking has
        a registered compensation, so by the time we arrive here the unwind has
        released the holds. We then re-book the disrupted leg on a more reliable
        mode (cab) rather than leaving the traveller with nothing.
        """
        ctx.state = "REPLANNING"

        # Pick the leg that actually needs replacing: the degraded one. Fall
        # back to the whole plan so a mid-booking disruption on an already
        # committed leg still yields a substitute.
        target = self._first_degraded_leg(ctx, legs) or (
            legs[-1] if legs else None
        )
        if target is None:
            workflow.logger.warning("degradation reported but no leg to replace")
            ctx.state = "REPLAN_NOOP"
            return

        leg_id = str(target.get("leg_id") or target.get("train_id") or "")
        workflow.logger.info(
            "re-planning: substituting %s (%s -> %s)",
            leg_id, target.get("origin"), target.get("destination"),
        )
        ctx.state = "FALLBACK"
        await self._fallback_to_cab(ctx, [target], reason)
        ctx.state = "FALLBACK_COMPLETED" if ctx.fallback_used else "FALLBACK_FAILED"

    @staticmethod
    def _first_degraded_leg(
        ctx: SagaContext, legs: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        for leg in legs:
            lid = str(leg.get("leg_id") or leg.get("train_id") or "")
            if lid and ctx.live_probability.get(lid, 1.0) < DEGRADED_THRESHOLD:
                return leg
        return None

    # ------------------------------------------------------------------ legs
    async def _book_one_leg(
        self, ctx: SagaContext, leg: Dict[str, Any], waitlist_probability: float
    ) -> None:
        """Register the undo step first, then execute the forward activity."""
        train_id = leg["train_id"]
        origin, destination = leg["origin"], leg["destination"]
        travel_time_min = int(leg.get("travel_time_min", 60))
        p = float(leg.get("waitlist_probability", waitlist_probability))

        # (1) COMPENSATION REGISTERED *BEFORE* THE FORWARD STEP.  The booking
        #     may partially commit before the worker dies, so the undo must
        #     already be on the stack.  cancel_train is idempotent and no-ops
        #     when nothing was booked, making pre-registration safe.
        ctx.register("cancel_train", cancel_train, ctx.itinerary_id, train_id)

        # (2) forward step.  WaitlistDroppedError is non-retryable, so it
        #     propagates straight to the handler in run().
        try:
            result = await workflow.execute_activity(
                book_train,
                args=[
                    ctx.itinerary_id, train_id, origin, destination,
                    travel_time_min, p,
                ],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(**RETRY_POLICY),
            )
        except ActivityError as exc:
            # The concrete exception type does not survive the activity
            # boundary: it arrives as ActivityError wrapping a Failure. Walk
            # the cause chain looking for our marker so the Saga handler in
            # run() can branch into the fallback path.
            if _is_waitlist_drop(exc):
                raise WaitlistDroppedError(train_id, ctx.itinerary_id) from exc
            raise

        ctx.booked.append(result)
        workflow.logger.info("booked leg %s -> %s", train_id, result.get("reference"))

    async def _fallback_to_cab(
        self, ctx: SagaContext, legs: List[Dict[str, Any]], reason: str
    ) -> None:
        """Run after a clean unwind: substitute a cab for the failed legs."""
        if not legs:
            workflow.logger.warning("no legs to substitute a cab for")
            return
        leg = legs[0]
        try:
            receipt = await workflow.execute_activity(
                book_cab,
                args=[
                    ctx.itinerary_id,
                    leg["origin"],
                    leg["destination"],
                    reason[:120],
                ],
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(**RETRY_POLICY),
            )
        except (ActivityError, ApplicationError, FailureError) as exc:
            # The fallback itself failed -- the itinerary is simply unservable.
            workflow.logger.error("cab fallback failed: %s", exc)
            ctx.state = "FALLBACK_FAILED"
            return
        ctx.fallback_used = True
        ctx.booked.append(receipt)
        workflow.logger.info("fallback cab booked: %s", receipt.get("reference"))

    async def _publish_events(self, ctx: SagaContext) -> None:
        for receipt in ctx.booked:
            try:
                await workflow.execute_activity(
                    publish_booking_event,
                    args=[receipt],
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        backoff_coefficient=2.0,
                        maximum_attempts=2,
                    ),
                )
            except (ActivityError, ApplicationError, FailureError) as exc:
                workflow.logger.warning("event publish failed: %s", exc)


        # Plain dict: the workflow must not depend on pydantic (see note above).
        return {
            "workflow_id": workflow.info().workflow_id,
            "state": ctx.state,
            "booked": ctx.booked,
            "compensations_run": ctx.compensations_run,
            "compensation_errors": ctx.compensation_errors,
            "failure": ctx.failure,
            "fallback_used": ctx.fallback_used,
            "solution": ctx.solution,
        }

