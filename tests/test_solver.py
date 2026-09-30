"""Smoke tests that need no external services.

Run with:  python -m tests.test_solver
(or:      pytest tests/test_solver.py)
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.models import (  # noqa: E402
    TEMPLE_WINDOWS,
    RoutingRequest,
    TimeWindow,
    TransitEdge,
    TransitNode,
)
from app.solver.cp_router import StochasticOrienteeringSolver  # noqa: E402


def test_disconnected_window_forces_either_session() -> None:
    """A temple reachable only in the afternoon must be timed into 16:00-21:00."""
    req = RoutingRequest(
        nodes=[
            TransitNode(node_id="DEPOT", name="Depot"),
            TransitNode(
                node_id="TEMPLE",
                name="Temple",
                windows=[TimeWindow(start_min=16 * 60, end_min=21 * 60)],
            ),
        ],
        edges=[
            TransitEdge(
                edge_id="e1", origin="DEPOT", destination="TEMPLE",
                travel_time_min=90, cost_inr=100.0, probability=1.0,
            )
        ],
        start_node_id="DEPOT",
        start_time_min=15 * 60,  # 15:00 + 90m = 16:30, inside the evening session
        day_budget_min=8 * 60,
        max_stops=1,
        required_node_ids=["TEMPLE"],
    )
    sol = StochasticOrienteeringSolver(5.0, 8).solve(req)
    assert sol.feasible, sol.status
    assert sol.visits[0].arrival_min == 16 * 60 + 30
    assert TEMPLE_WINDOWS[1].contains(sol.visits[0].arrival_min)


def test_infeasible_when_window_cannot_be_met() -> None:
    """Departing too late to reach the morning session => no itinerary."""
    req = RoutingRequest(
        nodes=[
            TransitNode(node_id="DEPOT", name="Depot"),
            TransitNode(
                node_id="TEMPLE", name="Temple",
                windows=[TimeWindow(start_min=6 * 60, end_min=8 * 60)],
            ),
        ],
        edges=[
            TransitEdge(edge_id="e1", origin="DEPOT", destination="TEMPLE",
                        travel_time_min=600, cost_inr=100.0)
        ],
        start_node_id="DEPOT",
        start_time_min=9 * 60,  # 09:00 + 10h = 19:00, past the 08:00 close
        day_budget_min=14 * 60,
        max_stops=1,
        required_node_ids=["TEMPLE"],
    )
    sol = StochasticOrienteeringSolver(5.0, 8).solve(req)
    assert not sol.feasible, "model should be infeasible"


def test_risk_weight_prefers_reliable_edge() -> None:
    """With a high risk weight the solver avoids the flaky waitlisted train."""
    req = RoutingRequest(
        nodes=[
            TransitNode(node_id="A", name="A"),
            TransitNode(node_id="B", name="B"),
        ],
        edges=[
            TransitEdge(edge_id="flaky", origin="A", destination="B",
                        travel_time_min=60, cost_inr=100.0, probability=0.10),
            TransitEdge(edge_id="safe", origin="A", destination="B",
                        travel_time_min=70, cost_inr=100.0, probability=1.0),
        ],
        start_node_id="A",
        start_time_min=6 * 60,
        day_budget_min=10 * 60,
        max_stops=1,
        required_node_ids=["B"],
        cost_weight=1.0,
        risk_weight=100.0,
    )
    sol = StochasticOrienteeringSolver(5.0, 8).solve(req)
    assert sol.feasible, sol.status
    assert sol.visits[0].incoming_edge_id == "safe", (
        f"expected the reliable edge, got {sol.visits[0].incoming_edge_id}"
    )


def test_multi_stop_circuit_from_demo_data() -> None:
    from app.sample_data import sample_request

    sol = StochasticOrienteeringSolver(8.0, 8).solve(sample_request())
    assert sol.feasible, sol.status
    assert 1 <= len(sol.visits) <= 4
    # every temple visit must land inside one of its two sessions
    by_id = {n.node_id: n for n in sample_request().nodes}
    for visit in sol.visits:
        node = by_id[visit.node_id]
        if node.windows:
            assert any(w.contains(visit.arrival_min) for w in node.windows), (
                f"{visit.node_id} arrived at {visit.arrival_min} outside all windows"
            )


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS  {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL  {name}: {exc}")
    print(f"\n{'ALL TESTS PASSED' if not failures else str(failures) + ' FAILURE(S)'}")
    sys.exit(1 if failures else 0)
