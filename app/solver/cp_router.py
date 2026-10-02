"""CP-SAT model for the Time-Dependent Stochastic Orienteering Problem.

The model decides *which* sequence of cities to visit and *when* to visit them,
subject to:

* a global day budget (time-dependent travel durations),
* optional service windows -- including **disconnected windows** where a node
  is only serviceable during e.g. 06:00-12:00 *or* 16:00-21:00 (South Indian
  temples that close for the afternoon session),
* a stochastic objective that prices in the risk of low-reliability
  (waitlisted / congestion-prone) edges.

Disconnected windows are encoded with one boolean literal per (node, window)
pair, an ``OnlyEnforceIf``-guarded interval constraint per pair, and a final
``model.AddBoolOr([...])`` forcing at least one window to be satisfied.

The heavy lifting is exposed as a Ray task so the FastAPI replica never blocks
its event loop.
"""
from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Sequence, Tuple

try:  # pragma: no cover - depends on which image is running
    import ray
except ImportError:  # API/worker image: Ray Client is intentionally absent
    # The API process never initialises Ray (Phase 6): solving is dispatched
    # over the Ray Jobs REST API, and the cluster image runs this module with
    # `ray` present. Here the decorator degrades to a plain function so
    # `solve_local` -- which the /route endpoint uses for the initial plan --
    # still imports and runs.
    ray = None  # type: ignore[assignment]

from ortools.sat.python import cp_model

from ..models import (
    RoutingRequest,
    RoutingSolution,
    TimeWindow,
    TransitEdge,
    TransitNode,
    Visit,
)

log = logging.getLogger("payanam.solver")

# Objective scaling: probabilities are floats, CP-SAT is integral.
_OBJECTIVE_SCALE = 100


# --------------------------------------------------------------------------- #
# OR-Tools naming compatibility
# --------------------------------------------------------------------------- #
# OR-Tools renamed the camelCase API (NewBoolVar / AddBoolOr) to snake_case in
# 9.8. Resolve each method once so the model reads identically across versions.
def _bool_var(model: cp_model.CpModel, name: str):
    maker = getattr(model, "NewBoolVar", None) or model.new_bool_var
    return maker(name)


def _add_bool_or(model: cp_model.CpModel, literals: Sequence[object]):
    adder = getattr(model, "AddBoolOr", None) or model.add_bool_or
    return adder(list(literals))


def _int_var(model: cp_model.CpModel, lb: int, ub: int, name: str):
    maker = getattr(model, "NewIntVar", None) or model.new_int_var
    return maker(lb, ub, name)


# --------------------------------------------------------------------------- #
# Core (pure, in-process) solver
# --------------------------------------------------------------------------- #
class StochasticOrienteeringSolver:
    """Builds and solves the CP-SAT routing model for a single state."""

    def __init__(
        self,
        time_limit_seconds: float = 5.0,
        num_workers: int = 8,
        log_search: bool = False,
    ) -> None:
        self.time_limit_seconds = time_limit_seconds
        self.num_workers = num_workers
        self.log_search = log_search

    # ------------------------------------------------------------------ util
    @staticmethod
    def _effective_travel(
        edge: TransitEdge, multipliers: Optional[Dict[str, float]]
    ) -> int:
        """Apply the real-time traffic multiplier pushed over Kafka."""
        m = 1.0
        if multipliers:
            m = float(
                multipliers.get(edge.edge_id, multipliers.get(edge.origin, 1.0))
            )
        m = min(max(m, 0.1), 10.0)
        return max(1, int(round(edge.travel_time_min * m)))

    # ----------------------------------------------------------------- build
    def build_model(
        self, request: RoutingRequest
    ) -> Tuple[cp_model.CpModel, Dict[str, object]]:
        """Construct the CpModel and return it with the variable handles."""
        model = cp_model.CpModel()

        nodes: List[TransitNode] = request.nodes
        index: Dict[str, int] = {n.node_id: i for i, n in enumerate(nodes)}
        n = len(nodes)
        start = index[request.start_node_id]
        horizon = request.start_time_min + request.day_budget_min

        # ---- adjacency (self loops dropped) ---------------------------------
        # adj[i] -> list of (destination_index, edge, effective_travel_minutes)
        adj: Dict[int, List[Tuple[int, TransitEdge, int]]] = {i: [] for i in range(n)}
        for edge in request.edges:
            oi, di = index[edge.origin], index[edge.destination]
            if oi == di:
                continue
            adj[oi].append(
                (di, edge, self._effective_travel(edge, request.traffic_multipliers))
            )

        # ---------------------------------------------------------- variables
        # x[i][k] -- boolean arc literal: "we travel i -> k"
        x: List[List[object]] = [[None] * n for _ in range(n)]  # type: ignore[list-item]
        for i in range(n):
            for k, _, _ in adj[i]:
                if k == start:
                    continue  # nothing travels back into the depot
                x[i][k] = _bool_var(model, f"x_{i}_{k}")

        # visited[i] -- boolean: node i is serviced in this itinerary
        visited = [_bool_var(model, f"visited_{i}") for i in range(n)]

        # arrival[i] -- absolute arrival time in minutes past midnight
        arrival = [_int_var(model, 0, horizon, f"arrival_{i}") for i in range(n)]
        model.Add(arrival[start] == request.start_time_min)

        # ------------------------------------------------ disconnected windows
        # One boolean literal per (node, window); the model then asserts that a
        # serviced node lands in at least one of its windows.
        window_hits: Dict[int, List[object]] = {}
        for i, node in enumerate(nodes):
            windows: List[TimeWindow] = node.windows
            if not windows:
                continue  # always-open node

            if len(windows) == 1:
                w = windows[0]
                model.Add(arrival[i] >= w.start_min).OnlyEnforceIf(visited[i])
                model.Add(arrival[i] <= w.end_min - 1).OnlyEnforceIf(visited[i])
                continue

            hits: List[object] = []
            for widx, w in enumerate(windows):
                lit = _bool_var(model, f"win_{i}_{widx}")
                hits.append(lit)
                # being in this window implies the node is actually serviced
                model.AddImplication(lit, visited[i])
                # ... and the arrival time lies strictly inside the window
                model.Add(arrival[i] >= w.start_min).OnlyEnforceIf(lit)
                model.Add(arrival[i] <= w.end_min - 1).OnlyEnforceIf(lit)

            # THE disconnected-window constraint: arrival in window 1 OR 2 OR ...
            _add_bool_or(model, hits)
            # ... and only one of them, for a tighter LP and deterministic ties
            for a_lit, b_lit in zip(hits, hits[1:]):
                model.AddBoolOr([a_lit.Not(), b_lit.Not()])
            window_hits[i] = hits

        # ---------------------------------------------------- time propagation
        for i in range(n):
            for k, _, tt in adj[i]:
                lit = x[i][k]
                if lit is None:
                    continue
                # time-dependent edge: the successor cannot be reached earlier
                model.Add(arrival[k] >= arrival[i] + tt).OnlyEnforceIf(lit)
                # and a selected arc forces the destination to be serviced
                model.AddImplication(lit, visited[k])

        # Each node has at most one predecessor and at most one successor.
        for k in range(n):
            incoming = [x[i][k] for i in range(n) if x[i][k] is not None]
            if incoming:
                model.Add(sum(incoming) <= 1)
            outgoing = [x[k][j] for j in range(n) if x[k][j] is not None]
            if outgoing:
                model.Add(sum(outgoing) <= 1)

        # ------------------------------------------------------ budget / start
        model.Add(visited[start] == 0)
        for i in range(n):
            if i == start:
                continue
            model.Add(arrival[i] >= request.start_time_min).OnlyEnforceIf(visited[i])
            model.Add(arrival[i] <= horizon).OnlyEnforceIf(visited[i])

        # Visit-count cap.
        model.Add(sum(visited) <= request.max_stops)

        # Mandatory service pins.
        for rid in request.required_node_ids:
            model.Add(visited[index[rid]] == 1)

        # A serviced node (other than the start) needs exactly one real
        # predecessor, otherwise the model could claim an unreachable visit.
        for i in range(n):
            if i == start:
                continue
            incoming = [x[j][i] for j in range(n) if x[j][i] is not None]
            if incoming:
                model.Add(sum(incoming) == 1).OnlyEnforceIf(visited[i])

        # ------------------------------------------------------- connectivity
        # Degree constraints alone permit *disconnected* components: the solver
        # could service a node with no path back to the depot, plus sub-tours.
        #
        # Flow formulation (x[i][k] is a 0/1 flow on arc i->k):
        #   * every serviced node has exactly one incoming arc,
        #   * a serviced node either continues (out=1) or terminates the walk
        #     (out=0, terminal=1),
        #   * the depot departs exactly once whenever anything is serviced.
        #
        # With in/out degree <= 1 this forces a single path leaving the depot
        # that covers exactly the visited set -- no sub-tours, no orphans.
        for i in range(n):
            incoming_i = [x[j][i] for j in range(n) if x[j][i] is not None]
            outgoing_i = [x[i][k] for k in range(n) if x[i][k] is not None]
            in_sum = sum(incoming_i) if incoming_i else 0
            out_sum = sum(outgoing_i) if outgoing_i else 0
            if i == start:
                # Exactly one departure whenever anything is serviced; this
                # also forces sum(terminal) == 1 for a non-empty walk.
                model.Add(request.max_stops * out_sum >= sum(visited))
            else:
                terminal = _bool_var(model, f"terminal_{i}")
                model.Add(in_sum == visited[i])              # exactly one way in
                model.Add(out_sum + terminal == visited[i])  # continue, or stop
                model.AddImplication(terminal, visited[i])

        # ------------------------------------------------------- objective
        # maximize  SUM(node reward for serviced nodes)
        # minimize  cost_weight * SUM(travel_time)
        #         + risk_weight  * SUM((1 - p_e) * travel_time_e)
        # All terms are integer-scaled so CP-SAT stays fully integral.
        travel_expr = 0
        risk_expr = 0
        cost_expr = 0.0
        for i in range(n):
            for k, edge, tt in adj[i]:
                lit = x[i][k]
                if lit is None:
                    continue
                travel_expr += (tt * _OBJECTIVE_SCALE) * lit
                risk_expr += int(round((1.0 - edge.probability) * tt * _OBJECTIVE_SCALE)) * lit
                cost_expr += edge.cost_inr * lit

        # Reward term: without it the empty tour is trivially cost-optimal.
        reward_expr = sum(
            int(round(nodes[i].reward * _OBJECTIVE_SCALE)) * visited[i]
            for i in range(n)
            if i != start
        )

        # travel_expr / risk_expr / reward_expr are all already scaled by
        # _OBJECTIVE_SCALE, so the weights are applied unscaled here.
        model.Minimize(
            int(round(request.cost_weight)) * travel_expr
            + int(round(request.risk_weight)) * risk_expr
            - reward_expr
        )

        handles: Dict[str, object] = {
            "nodes": nodes,
            "index": index,
            "x": x,
            "visited": visited,
            "arrival": arrival,
            "window_hits": window_hits,
            "travel_expr": travel_expr,
            "risk_expr": risk_expr,
            "cost_expr": cost_expr,
            "start": start,
            "horizon": horizon,
        }
        return model, handles


    # ------------------------------------------------------------------ solve
    def solve(self, request: RoutingRequest) -> RoutingSolution:
        started = time.perf_counter()
        model, h = self.build_model(request)
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = self.time_limit_seconds
        solver.parameters.num_search_workers = self.num_workers
        solver.parameters.log_search_progress = self.log_search

        status = solver.Solve(model)
        wall_ms = int((time.perf_counter() - started) * 1000)
        status_name = solver.StatusName(status)

        if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            log.warning("no feasible itinerary found: %s", status_name)
            return RoutingSolution(
                feasible=False, status=status_name, solver_wall_time_ms=wall_ms
            )

        nodes: List[TransitNode] = h["nodes"]           # type: ignore[assignment]
        index: Dict[str, int] = h["index"]              # type: ignore[assignment]
        x = h["x"]
        arrival = h["arrival"]

        selected: List[Tuple[int, int]] = [
            (i, k)
            for i in range(len(nodes))
            for k in range(len(nodes))
            if x[i][k] is not None and solver.BooleanValue(x[i][k])
        ]

        # Order the selected arcs into a path starting at the start node.
        by_origin: Dict[int, int] = {}
        for i, k in selected:
            by_origin.setdefault(i, k)
        # Include the depot at index 0 so the first leg keeps its incoming edge.
        path: List[int] = [h["start"]]                        # type: ignore[list-item]
        cursor = h["start"]                                   # type: ignore[assignment]
        seen = {cursor}
        while cursor in by_origin:
            cursor = by_origin[cursor]
            if cursor in seen:  # defensive: the model forbids cycles, but be safe
                break
            seen.add(cursor)
            path.append(cursor)

        edge_lookup: Dict[Tuple[int, int], TransitEdge] = {
            (index[e.origin], index[e.destination]): e for e in request.edges
        }
        visits: List[Visit] = []
        total_travel = 0
        total_cost = 0.0
        expected_risk = 0.0
        for pos, node_i in enumerate(path):
            if pos == 0:
                continue  # the depot itself is not a visit
            incoming = edge_lookup.get((path[pos - 1], node_i))
            arr = int(solver.Value(arrival[node_i]))
            if incoming is not None:
                tt = self._effective_travel(incoming, request.traffic_multipliers)
                total_travel += tt
                total_cost += incoming.cost_inr
                expected_risk += (1.0 - incoming.probability) * tt
            visits.append(
                Visit(
                    node_id=nodes[node_i].node_id,
                    arrival_min=arr,
                    departure_min=arr,
                    incoming_edge_id=incoming.edge_id if incoming else None,
                    edge_probability=incoming.probability if incoming else 1.0,
                )
            )

        return_min = visits[-1].arrival_min if visits else request.start_time_min
        return RoutingSolution(
            feasible=True,
            visits=visits,
            total_travel_time_min=total_travel,
            total_cost_inr=round(total_cost, 2),
            stochastic_penalty=round(expected_risk * request.risk_weight, 4),
            objective_value=round(
                request.cost_weight * total_travel
                + request.risk_weight * expected_risk,
                4,
            ),
            return_min=return_min,
            status=status_name,
            solver_wall_time_ms=wall_ms,
        )


# --------------------------------------------------------------------------- #
# Ray-distributed entrypoint
# --------------------------------------------------------------------------- #
def solve_routing_task(
    payload: Dict[str, object],
    time_limit_seconds: float = 5.0,
    num_workers: int = 8,
) -> Dict[str, object]:
    """Ray task: deserialise, solve, and return a plain dict.

    A dict (rather than a Pydantic model) keeps the payload trivially picklable
    across the Ray object store without custom codecs.
    """
    request = RoutingRequest.model_validate(payload)
    solver = StochasticOrienteeringSolver(
        time_limit_seconds=time_limit_seconds, num_workers=num_workers
    )
    return solver.solve(request).model_dump()


# On the cluster image this becomes a Ray task; in the API image (no `ray`
# package) it stays an ordinary callable, so importing this module for
# `solve_local` never requires the Ray Client. The Ray image's job_entrypoint
# is the only caller that uses ray.get() on it.
if ray is not None:  # pragma: no branch
    solve_routing_task = ray.remote(solve_routing_task)  # type: ignore[assignment]


# Local (non-Ray) convenience wrapper for tests and CLI use.
def solve_local(
    request: RoutingRequest,
    time_limit_seconds: float = 5.0,
    num_workers: int = 8,
) -> RoutingSolution:
    return StochasticOrienteeringSolver(time_limit_seconds, num_workers).solve(request)
