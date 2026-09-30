"""API v1 routes for Project payanam.

``POST /api/v1/route`` validates the routing problem, optionally seeds it from
Memgraph, and starts the ``ItinerarySaga`` Temporal workflow.  It never solves
inline -- the CP-SAT work is dispatched to Ray *inside* the workflow.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from ..config import get_settings
from ..ingestion.stream import REGISTRY
from ..models import RouteResponse, RouteResult, RoutingRequest
from ..sample_data import sample_request
from ..temporal_client import get_temporal_client, start_itinerary_workflow

log = logging.getLogger("payanam.api")
router = APIRouter(prefix="/api/v1", tags=["routing"])


# --------------------------------------------------------------------------- #
# Request/response envelopes
# --------------------------------------------------------------------------- #
class RouteCommand(BaseModel):
    """The body accepted by ``POST /api/v1/route``."""

    request: Optional[RoutingRequest] = Field(
        default=None,
        description="Full problem instance. Omit to use the built-in demo circuit.",
    )
    use_demo_data: bool = Field(
        default=False, description="Shortcut for the bundled South-India circuit."
    )
    waitlist_probability: float = Field(
        default=0.0, ge=0.0, le=1.0,
        description="Simulated waitlist drop rate for the rail legs.",
    )
    legs: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Explicit booking legs. Defaults to the solver's chosen arcs.",
    )
    apply_live_traffic: bool = Field(
        default=True,
        description="Fold in multipliers received on the traffic_updates topic.",
    )

    def resolved_request(self) -> RoutingRequest:
        if self.use_demo_data or self.request is None:
            return sample_request()
        return self.request


class RouteStatusResponse(BaseModel):
    workflow_id: str
    run_id: str


# --------------------------------------------------------------------------- #
# Leg derivation
# --------------------------------------------------------------------------- #
def _legs_from_solution(req: RoutingRequest) -> List[Dict[str, Any]]:
    """Turn the solver's chosen path into bookable legs.

    Phase 2 resolves this properly: the CP-SAT solution is computed
    in-process (it is fast, and the endpoint must return an *initial plan*
    immediately), then each visited node's incoming edge becomes a booking leg
    carrying its mode and live ``waitlist_probability``. That is what the
    workflow books, and what a later ``transit_update`` signal degrades.
    """
    from ..solver.cp_router import solve_local

    solution = solve_local(req, time_limit_seconds=3.0)
    if not solution.feasible:
        log.warning("no feasible itinerary (status=%s); empty plan", solution.status)
        return []

    edges = {e.edge_id: e for e in req.edges}
    nodes = {n.node_id: n for n in req.nodes}
    legs: List[Dict[str, Any]] = []
    for visit in solution.visits:
        edge = edges.get(visit.incoming_edge_id or "")
        if edge is None:
            continue
        node = nodes.get(visit.node_id)
        legs.append(
            {
                "leg_id": edge.edge_id,
                "train_id": edge.edge_id,
                "origin": edge.origin,
                "destination": edge.destination,
                "mode": edge.mode,
                "travel_time_min": edge.travel_time_min,
                "cost_inr": edge.cost_inr,
                # the live confirmation probability the solver assumed
                "waitlist_probability": edge.probability,
                "arrival_min": visit.arrival_min,
                "node_type": node.node_type.value if node else None,
            }
        )
    return legs


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@router.post(
    "/route",
    response_model=RouteResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start an itinerary Saga (async)",
)
async def create_route(command: RouteCommand) -> RouteResponse:
    """Validate the problem and start the ``ItinerarySaga`` workflow.

    Returns 202 with the workflow identifiers; poll
    ``GET /api/v1/route/{workflow_id}`` for the terminal result.
    """
    settings = get_settings()
    try:
        req = command.resolved_request()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Fold in live edge multipliers produced by the Kafka consumer.
    if command.apply_live_traffic and REGISTRY.multipliers:
        req = req.model_copy(
            update={"traffic_multipliers": {**REGISTRY.snapshot(), **req.traffic_multipliers}}
        )

    client = get_temporal_client()
    itinerary_id = uuid.uuid4().hex[:16]
    legs = command.legs or _legs_from_solution(req)

    try:
        handle = await start_itinerary_workflow(
            client=client,
            settings=settings,
            itinerary_id=itinerary_id,
            legs=legs,
            request_payload=req.model_dump(mode="json"),
            waitlist_probability=command.waitlist_probability,
            replan_grace_seconds=settings.replan_grace_seconds,
        )
    except Exception as exc:  # noqa: BLE001 - surfaced to the caller
        log.exception("failed to start workflow")
        raise HTTPException(
            status_code=503, detail=f"could not start workflow: {exc}"
        ) from exc

    return RouteResponse(
        workflow_id=handle.id,
        run_id=handle.first_execution_run_id or "",
        task_queue=settings.temporal_task_queue,
        status="STARTED",
        message=(
            "ItinerarySaga started. Poll GET /api/v1/route/"
            f"{handle.id} for the result, or GET /api/v1/itinerary/"
            f"{handle.id} for the live plan."
        ),
        initial_plan=legs,
        itinerary_id=itinerary_id,
    )


@router.get(
    "/route/{workflow_id}",
    response_model=RouteStatusResponse,
    summary="Fetch itinerary Saga status and result",
)
async def get_route(workflow_id: str) -> RouteStatusResponse:
    client = get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)
    try:
        description = await handle.describe()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"unknown workflow: {exc}") from exc

    status_name = description.status.name if description.status else "UNKNOWN"
    result: Optional[RouteResult] = None
    state = "UNKNOWN"
    if description.status and description.status.name == "COMPLETED":
        try:
            result = RouteResult.model_validate(await handle.result())
            state = result.state
        except Exception as exc:  # noqa: BLE001
            log.warning("could not decode workflow result: %s", exc)

    return RouteStatusResponse(
        workflow_id=workflow_id,
        run_id=description.run_id or "",
        status=status_name,
        state=state,
        result=result,
    )


@router.get(
    "/itinerary/{workflow_id}",
    summary="Live itinerary (workflow query)",
)
async def live_itinerary(workflow_id: str) -> Dict[str, Any]:
    """Read the in-flight plan straight from the workflow.

    Proxies the ``get_current_itinerary`` query so the caller sees booked legs,
    pending legs and any live probability degradations without waiting for the
    workflow to complete.
    """
    client = get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)
    try:
        return await handle.query("get_current_itinerary")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=409, detail=f"workflow not queryable: {exc}"
        ) from exc


@router.get("/compensations/{workflow_id}", summary="Pending Saga compensations")
async def get_compensations(workflow_id: str) -> Dict[str, Any]:
    """Query the workflow's live compensation stack (Saga introspection)."""
    client = get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)
    try:
        from ..workflows.itinerary import ItineraryWorkflow

        stack = await handle.query(ItineraryWorkflow.compensation_stack)
        state = await handle.query(ItineraryWorkflow.saga_state)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=409, detail=f"workflow not queryable: {exc}"
        ) from exc
    return {"workflow_id": workflow_id, "pending": stack, "state": state}


@router.get("/graph/schema", summary="MATCH (a:City)-[r:TRANSIT]->(b:City) RETURN r")
async def graph_schema(limit: int = Query(default=50, ge=1, le=500)) -> Dict[str, Any]:
    """Probe the live Memgraph transit schema."""
    from ..graph.state import AsyncMemgraphClient

    client = AsyncMemgraphClient()
    try:
        await client.connect()
        rows = await client.transit_schema()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"memgraph unreachable: {exc}") from exc
    finally:
        await client.close()
    return {"count": len(rows), "rows": rows[:limit]}


@router.get("/traffic/live", summary="Current edge-weight multipliers")
async def live_traffic() -> Dict[str, Any]:
    """Multipliers currently held by the Kafka ingestion consumer."""
    return {
        "events_seen": REGISTRY.events_seen,
        "multipliers": REGISTRY.snapshot(),
    }

    status: str
    state: str = "UNKNOWN"
    result: Optional[RouteResult] = None
