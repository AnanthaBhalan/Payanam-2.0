"""Guards against Pydantic models that fail to resolve at runtime.

``from __future__ import annotations`` turns every annotation into a string, so
a model referencing ``Any`` (or any type not imported into the module) stays
*unresolved* until something instantiates it. Import succeeds; construction
raises ``PydanticUserError: ... is not fully defined``.

That bit us with ``RouteResponse.initial_plan: List[Dict[str, Any]]`` -- a 500
on ``POST /api/v1/route`` that no test caught, because nothing constructed the
model. These tests construct every response model.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.models import (  # noqa: E402
    RouteResponse,
    RouteResult,
    RoutingRequest,
    RoutingSolution,
    TimeWindow,
    TransitEdge,
    TransitNode,
    Visit,
)


def test_route_response_constructs() -> None:
    """POST /api/v1/route builds this; it must resolve."""
    resp = RouteResponse(
        workflow_id="payanam-abc",
        run_id="run-1",
        task_queue="payanam-routing",
        status="STARTED",
        message="ok",
        itinerary_id="abc",
        initial_plan=[{"leg_id": "tn_vaigai_mas_mdu", "origin": "MAS"}],
    )
    assert resp.initial_plan[0]["leg_id"] == "tn_vaigai_mas_mdu"
    assert resp.itinerary_id == "abc"


def test_route_response_plan_defaults_to_empty() -> None:
    resp = RouteResponse(
        workflow_id="w", run_id="r", task_queue="q", status="S", message="m"
    )
    assert resp.initial_plan == []


def test_route_result_constructs() -> None:
    result = RouteResult(
        workflow_id="payanam-abc",
        state="COMPLETED",
        booked=[{"kind": "TRAIN"}],
        compensations_run=[],
        fallback_used=False,
    )
    assert result.state == "COMPLETED"


def test_routing_solution_and_domain_types_construct() -> None:
    sol = RoutingSolution(
        feasible=True,
        visits=[
            Visit(node_id="MDU", arrival_min=540, departure_min=540, edge_probability=1.0)
        ],
        total_travel_time_min=180,
    )
    assert sol.feasible and sol.visits[0].node_id == "MDU"

    edge = TransitEdge(
        edge_id="e", origin="MAS", destination="MDU",
        travel_time_min=540, cost_inr=780.0, probability=0.45,
    )
    node = TransitNode(
        node_id="MDU", name="Madurai", windows=[TimeWindow(start_min=300, end_min=750)]
    )
    depot = TransitNode(node_id="MAS", name="Chennai Central")
    req = RoutingRequest(
        nodes=[depot, node], edges=[edge], start_node_id="MAS"
    )
    assert req.start_node_id == "MAS"
    assert len(req.nodes) == 2


def test_time_window_rejects_inverted_interval() -> None:
    with pytest.raises(ValueError):
        TimeWindow(start_min=700, end_min=600)


@pytest.mark.parametrize(
    "model",
    [RouteResponse, RouteResult, RoutingSolution],
)
def test_models_have_resolved_schemas(model) -> None:
    """A missing forward-ref would leave ``__pydantic_complete__`` False."""
    assert model.__pydantic_complete__, f"{model.__name__} is not fully defined"