"""Domain models shared across the solver, the graph layer and the workflows."""
from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# --------------------------------------------------------------------------- #
# Time windows
# --------------------------------------------------------------------------- #
class TimeWindow(BaseModel):
    """A half-open interval ``[start_min, end_min)`` in minutes past midnight.

    Used to model *disconnected* availability: a South Indian temple node, for
    example, is serviceable 06:00-12:00 and 16:00-21:00 with a hard midday
    closure in between.
    """

    start_min: int = Field(ge=0, le=24 * 60)
    end_min: int = Field(ge=0, le=24 * 60)

    @model_validator(mode="after")
    def _check(self) -> "TimeWindow":
        if self.end_min <= self.start_min:
            raise ValueError(
                f"time window end ({self.end_min}) must be after start ({self.start_min})"
            )
        return self

    def contains(self, minute: int) -> bool:
        return self.start_min <= minute < self.end_min

    @staticmethod
    def hhmm_to_min(hhmm: str) -> int:
        hh, mm = hhmm.split(":")
        return int(hh) * 60 + int(mm)

    @staticmethod
    def min_to_hhmm(minute: int) -> str:
        return f"{minute // 60:02d}:{minute % 60:02d}"

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        return f"{self.min_to_hhmm(self.start_min)}-{self.min_to_hhmm(self.end_min)}"


# The canonical disconnected window: temples shut for the afternoon session.
TEMPLE_MORNING = TimeWindow(start_min=6 * 60, end_min=12 * 60)
TEMPLE_EVENING = TimeWindow(start_min=16 * 60, end_min=21 * 60)
TEMPLE_WINDOWS: List[TimeWindow] = [TEMPLE_MORNING, TEMPLE_EVENING]


# --------------------------------------------------------------------------- #
# Graph primitives
# --------------------------------------------------------------------------- #
class TransitEdge(BaseModel):
    """A stochastic travel option between two cities.

    ``probability`` is the *on-time success* probability used to build the
    stochastic penalty term of the CP-SAT objective.
    """

    edge_id: str
    origin: str
    destination: str
    mode: str = "TRAIN"  # TRAIN | BUS | CAB | FLIGHT
    travel_time_min: int = Field(ge=1)
    cost_inr: float = Field(ge=0.0)
    probability: float = Field(default=1.0, ge=0.0, le=1.0)
    # True when the edge is a *waitlisted* service (rail reservations) -- these
    # are the ones the Saga has to compensate.
    waitlisted: bool = False

    @field_validator("probability")
    @classmethod
    def _clamp(cls, v: float) -> float:
        return min(max(v, 0.0), 1.0)

    @property
    def expected_cost(self) -> float:
        return self.cost_inr / max(self.probability, 1e-6)


class NodeType(str, Enum):
    WAYPOINT = "WAYPOINT"
    TEMPLE = "TEMPLE"
    DEPOT = "DEPOT"


class TransitNode(BaseModel):
    node_id: str
    name: str
    node_type: NodeType = NodeType.WAYPOINT
    # Windows in which the node may be *visited*. An empty list means the node
    # is always open. Multiple entries == disconnected availability.
    windows: List[TimeWindow] = Field(default_factory=list)
    # Service reward. Without a positive reward the cost-minimising objective
    # would always return the empty tour, so every node carries a value that
    # the solver trades off against travel time and stochastic risk.
    reward: float = Field(default=100.0, ge=0.0)

    def is_open_at(self, minute: int) -> bool:
        if not self.windows:
            return True
        return any(w.contains(minute) for w in self.windows)


# --------------------------------------------------------------------------- #
# Solver request / response
# --------------------------------------------------------------------------- #
class RoutingRequest(BaseModel):
    """Time-Dependent Stochastic Orienteering problem instance."""

    nodes: List[TransitNode]
    edges: List[TransitEdge]
    start_node_id: str
    start_time_min: int = Field(default=6 * 60, ge=0, le=24 * 60)
    day_budget_min: int = Field(default=15 * 60, ge=1)
    max_stops: int = Field(default=8, ge=1)
    # Global risk appetite; scales the low-probability penalty in the objective.
    # Calibrated against the default node reward (100.0) and a cost_weight of
    # 1.0 per travel-minute, so a value in the 1-20 band makes reliability
    # matter without making the penalty dominate every service reward.
    cost_weight: float = Field(default=1.0, ge=0.0)
    risk_weight: float = Field(default=5.0, ge=0.0)
    # Optional pin of nodes that must be serviced.
    required_node_ids: List[str] = Field(default_factory=list)
    # Real-time multipliers pushed by the Kafka edge-weight stream.
    traffic_multipliers: Dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self) -> "RoutingRequest":
        ids = {n.node_id for n in self.nodes}
        if self.start_node_id not in ids:
            raise ValueError(f"unknown start_node_id '{self.start_node_id}'")
        missing = set(self.required_node_ids) - ids
        if missing:
            raise ValueError(f"required nodes not present: {sorted(missing)}")
        for e in self.edges:
            if e.origin not in ids or e.destination not in ids:
                raise ValueError(f"edge {e.edge_id} references unknown node")
        return self


class Visit(BaseModel):
    node_id: str
    arrival_min: int
    departure_min: int
    incoming_edge_id: Optional[str] = None
    edge_probability: float = 1.0


class RoutingSolution(BaseModel):
    feasible: bool
    visits: List[Visit] = Field(default_factory=list)
    total_travel_time_min: int = 0
    total_cost_inr: float = 0.0
    objective_value: float = 0.0
    stochastic_penalty: float = 0.0
    return_min: int = 0
    status: str = "UNKNOWN"
    solver_wall_time_ms: int = 0


class RouteResponse(BaseModel):
    workflow_id: str
    run_id: str
    task_queue: str
    status: str
    message: str
    # Phase 2: the initial plan, so the caller can see what was scheduled
    # before any live disruption arrives.
    initial_plan: List[Dict[str, Any]] = Field(default_factory=list)
    itinerary_id: str = ""


class RouteResult(BaseModel):
    """Terminal payload of the itinerary workflow."""

    workflow_id: str
    state: str
    booked: List[Dict[str, object]] = Field(default_factory=list)
    compensations_run: List[str] = Field(default_factory=list)
    fallback_used: bool = False
    solution: Optional[RoutingSolution] = None
