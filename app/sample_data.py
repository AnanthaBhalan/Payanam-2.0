"""Demo state-wide dataset: a South-Indian temple circuit with waitlisted rail.

Temples are modelled with *disconnected* service windows -- 06:00-12:00 and
16:00-21:00 -- so the CP-SAT model is forced to prove the itinerary can land
inside one of the two sessions.
"""
from __future__ import annotations

from typing import List

from .models import (
    TEMPLE_WINDOWS,
    NodeType,
    RoutingRequest,
    TimeWindow,
    TransitEdge,
    TransitNode,
)


def sample_nodes() -> List[TransitNode]:
    return [
        TransitNode(
            node_id="CHN", name="Chennai Central", node_type=NodeType.DEPOT
        ),
        TransitNode(
            node_id="KUM",
            name="Kumbakonam (temple)",
            node_type=NodeType.TEMPLE,
            windows=TEMPLE_WINDOWS,
            reward=600.0,
        ),
        TransitNode(
            node_id="TAN", name="Thanjavur Brihadeeswara", node_type=NodeType.TEMPLE,
            windows=[TimeWindow(start_min=6 * 60, end_min=12 * 60),
                     TimeWindow(start_min=16 * 60, end_min=20 * 60)],
            reward=500.0,
        ),
        TransitNode(
            node_id="TRP", name="Tiruchirappalli Rock Fort", node_type=NodeType.TEMPLE,
            windows=TEMPLE_WINDOWS,
            reward=400.0,
        ),
        TransitNode(
            node_id="MDU", name="Madurai Meenakshi", node_type=NodeType.TEMPLE,
            windows=[TimeWindow(start_min=5 * 60, end_min=12 * 60),
                     TimeWindow(start_min=16 * 60, end_min=21 * 60)],
            reward=700.0,
        ),
        TransitNode(
            node_id="PON", name="Pondicherry promenade", node_type=NodeType.WAYPOINT,
            reward=200.0,
        ),
    ]


def sample_edges() -> List[TransitEdge]:
    return [
        TransitEdge(edge_id="e_chn_tan", origin="CHN", destination="TAN",
                    mode="TRAIN", travel_time_min=180, cost_inr=450.0,
                    probability=0.92, waitlisted=True),
        TransitEdge(edge_id="e_chn_kum", origin="CHN", destination="KUM",
                    mode="TRAIN", travel_time_min=330, cost_inr=720.0,
                    probability=0.55, waitlisted=True),   # unreliable waitlist
        TransitEdge(edge_id="e_tan_kum", origin="TAN", destination="KUM",
                    mode="BUS", travel_time_min=90, cost_inr=280.0,
                    probability=0.97),
        TransitEdge(edge_id="e_kum_trp", origin="KUM", destination="TRP",
                    mode="TRAIN", travel_time_min=150, cost_inr=380.0,
                    probability=0.88, waitlisted=True),
        TransitEdge(edge_id="e_tan_trp", origin="TAN", destination="TRP",
                    mode="TRAIN", travel_time_min=200, cost_inr=500.0,
                    probability=0.95),
        TransitEdge(edge_id="e_trp_mdu", origin="TRP", destination="MDU",
                    mode="TRAIN", travel_time_min=240, cost_inr=610.0,
                    probability=0.80, waitlisted=True),
        TransitEdge(edge_id="e_chn_pon", origin="CHN", destination="PON",
                    mode="BUS", travel_time_min=170, cost_inr=300.0,
                    probability=0.99),
        TransitEdge(edge_id="e_pon_tan", origin="PON", destination="TAN",
                    mode="CAB", travel_time_min=240, cost_inr=3400.0,
                    probability=1.0),
        TransitEdge(edge_id="e_kum_mdu", origin="KUM", destination="MDU",
                    mode="BUS", travel_time_min=360, cost_inr=900.0,
                    probability=0.90),
    ]


def sample_request() -> RoutingRequest:
    return RoutingRequest(
        nodes=sample_nodes(),
        edges=sample_edges(),
        start_node_id="CHN",
        start_time_min=6 * 60,
        day_budget_min=14 * 60,
        max_stops=4,
        cost_weight=1.0,
        risk_weight=5.0,
        required_node_ids=[],
    )

