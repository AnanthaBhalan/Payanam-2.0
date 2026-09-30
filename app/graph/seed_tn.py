"""Realistic Tamil Nadu transit topology.

Five hubs, each carrying the metadata the solver needs:

* ``MAS`` Chennai Central (depot, always open)
* ``TPJ`` Tiruchirappalli  (Rock Fort)
* ``MDU`` Madurai          (Meenakshi Temple -- *disconnected* windows)
* ``KMU`` Kumbakonam       (temples -- *disconnected* windows)
* ``RMM`` Rameswaram       (Ramanathaswamy -- a single, long window)

Windows are half-open ``[start, end)`` in minutes past midnight, matching
:class:`app.models.TimeWindow`.  The Mandate's Meenakshi windows
(05:00-12:30 and 16:00-22:00) are expressed as ``(300, 750)`` and
``(960, 1320)``.

``probability`` is the on-time confirmation probability of the service and is
the value the reactive layer degrades when a live disruption arrives.  Vaigai
Express is deliberately fragile (0.45) versus the SETC bus (1.0) so the solver
must trade reliability against travel time.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Sequence, Tuple

from ..models import NodeType, RoutingRequest, TimeWindow, TransitEdge, TransitNode

log = logging.getLogger("payanam.graph")

# --------------------------------------------------------------------------- #
# Windows
# --------------------------------------------------------------------------- #
MEENAKSHI_WINDOWS: List[Tuple[int, int]] = [(5 * 60, 12 * 60 + 30), (16 * 60, 22 * 60)]
KUMBAKONAM_WINDOWS: List[Tuple[int, int]] = [(6 * 60, 12 * 60), (16 * 60, 20 * 60)]
ROCKFORT_WINDOWS: List[Tuple[int, int]] = [(6 * 60, 12 * 60), (16 * 60, 21 * 60)]
RAMANATHASWAMY_WINDOW: List[Tuple[int, int]] = [(4 * 60 + 30, 13 * 60)]

MAS = "MAS"
TPJ = "TPJ"
MDU = "MDU"
KMU = "KMU"
RMM = "RMM"

NODE_IDS: Tuple[str, ...] = (MAS, TPJ, MDU, KMU, RMM)

# Hard coordinates for each hub (lon, lat). The Mandate pins Chennai Central at
# 80.2707E, 13.0827N; the rest are the real station/temple locations, which is
# what lets ``book_cab`` turn a hub code into a real pickup point for the fleet
# matcher.
HUB_COORDS: Dict[str, Tuple[float, float]] = {
    MAS: (80.2707, 13.0827),   # Chennai Central
    TPJ: (78.7047, 10.7905),   # Tiruchirappalli Junction
    MDU: (78.1193, 9.9252),    # Madurai Junction
    KMU: (79.5639, 10.9500),   # Kumbakonam
    RMM: (79.3134, 9.2881),    # Rameswaram
}


def hub_coords(hub: str) -> Tuple[float, float]:
    """Resolve a hub code to ``(lon, lat)``.

    Accepts a bare code (``"MAS"``) or anything containing one, so callers can
    pass ``"tn_vaigai_mas_mdu"`` or ``"MAS"`` interchangeably.
    Raises :class:`KeyError` for an unknown hub -- an unroutable pickup is a
    terminal condition, not something to guess at.
    """
    if hub in HUB_COORDS:
        return HUB_COORDS[hub]
    upper = str(hub).upper()
    if upper in HUB_COORDS:
        return HUB_COORDS[upper]
    for code, coords in HUB_COORDS.items():
        if code in upper:
            return coords
    raise KeyError(f"no coordinates known for hub {hub!r}")



def _to_windows(raw: Sequence[Tuple[int, int]]) -> List[TimeWindow]:
    """Convert ``[(start, end), ...]`` into :class:`TimeWindow` instances."""
    return [TimeWindow(start_min=s, end_min=e) for s, e in raw]


def tn_nodes() -> List[TransitNode]:
    """The five Tamil Nadu hubs."""
    return [
        TransitNode(
            node_id=MAS,
            name="Chennai Central (MAS)",
            node_type=NodeType.DEPOT,
            reward=0.0,  # the depot is never a service target
        ),
        TransitNode(
            node_id=TPJ,
            name="Tiruchirappalli Rock Fort (TPJ)",
            node_type=NodeType.TEMPLE,
            windows=_to_windows(ROCKFORT_WINDOWS),
            reward=350.0,
        ),
        TransitNode(
            node_id=MDU,
            name="Madurai Meenakshi Temple (MDU)",
            node_type=NodeType.TEMPLE,
            windows=_to_windows(MEENAKSHI_WINDOWS),
            reward=600.0,
        ),
        TransitNode(
            node_id=KMU,
            name="Kumbakonam Temple City (KMU)",
            node_type=NodeType.TEMPLE,
            windows=_to_windows(KUMBAKONAM_WINDOWS),
            reward=500.0,
        ),
        TransitNode(
            node_id=RMM,
            name="Rameswaram Ramanathaswamy (RMM)",
            node_type=NodeType.TEMPLE,
            # a single, long session: exercises the one-window code path
            windows=_to_windows(RAMANATHASWAMY_WINDOW),
            reward=450.0,
        ),
    ]

def tn_edges() -> List[TransitEdge]:
    """Directed :TRANSIT options across the network."""
    return [
        # --- Chennai -> Madurai: the headline reliability trade-off --------
        TransitEdge(
            edge_id="tn_vaigai_mas_mdu", origin=MAS, destination=MDU,
            mode="TRAIN", travel_time_min=540, cost_inr=780.0,
            probability=0.45, waitlisted=True,   # Vaigai Express: fragile
        ),
        TransitEdge(
            edge_id="tn_setc_mas_mdu", origin=MAS, destination=MDU,
            mode="BUS", travel_time_min=480, cost_inr=950.0,
            probability=1.00, waitlisted=False,  # SETC: always confirms
        ),
        TransitEdge(
            edge_id="tn_cab_mas_mdu", origin=MAS, destination=MDU,
            mode="CAB", travel_time_min=390, cost_inr=6800.0,
            probability=1.00,
        ),
        # --- Chennai -> Trichy ---------------------------------------------
        TransitEdge(
            edge_id="tn_rockstar_mas_tpj", origin=MAS, destination=TPJ,
            mode="TRAIN", travel_time_min=330, cost_inr=520.0,
            probability=0.90, waitlisted=True,
        ),
        TransitEdge(
            edge_id="tn_bus_mas_tpj", origin=MAS, destination=TPJ,
            mode="BUS", travel_time_min=400, cost_inr=650.0,
            probability=0.99,
        ),
        # --- Trichy -> Madurai --------------------------------------------
        TransitEdge(
            edge_id="tn_cheran_tpj_mdu", origin=TPJ, destination=MDU,
            mode="TRAIN", travel_time_min=240, cost_inr=430.0,
            probability=0.80, waitlisted=True,
        ),
        TransitEdge(
            edge_id="tn_cab_tpj_mdu", origin=TPJ, destination=MDU,
            mode="CAB", travel_time_min=330, cost_inr=5200.0,
            probability=1.00,
        ),
        # --- Trichy <-> Kumbakonam ----------------------------------------
        TransitEdge(
            edge_id="tn_bus_tpj_kmu", origin=TPJ, destination=KMU,
            mode="BUS", travel_time_min=150, cost_inr=320.0,
            probability=0.95,
        ),
        TransitEdge(
            edge_id="tn_bus_kmu_tpj", origin=KMU, destination=TPJ,
            mode="BUS", travel_time_min=165, cost_inr=340.0,
            probability=0.95,
        ),
        # --- Madurai -> Rameswaram ----------------------------------------
        TransitEdge(
            edge_id="tn_train_mdu_rmm", origin=MDU, destination=RMM,
            mode="TRAIN", travel_time_min=180, cost_inr=240.0,
            probability=0.85, waitlisted=True,
        ),
        TransitEdge(
            edge_id="tn_bus_mdu_rmm", origin=MDU, destination=RMM,
            mode="BUS", travel_time_min=210, cost_inr=300.0,
            probability=0.98,
        ),
        # --- Madurai <-> Kumbakonam ----------------------------------------
        TransitEdge(
            edge_id="tn_bus_mdu_kmu", origin=MDU, destination=KMU,
            mode="BUS", travel_time_min=360, cost_inr=700.0,
            probability=0.92,
        ),
        TransitEdge(
            edge_id="tn_bus_kmu_mdu", origin=KMU, destination=MDU,
            mode="BUS", travel_time_min=370, cost_inr=720.0,
            probability=0.92,
        ),
        # --- Kumbakonam -> Rameswaram -------------------------------------
        TransitEdge(
            edge_id="tn_bus_kmu_rmm", origin=KMU, destination=RMM,
            mode="BUS", travel_time_min=240, cost_inr=380.0,
            probability=0.94,
        ),
        TransitEdge(
            edge_id="tn_train_rmm_kmu", origin=RMM, destination=KMU,
            mode="TRAIN", travel_time_min=210, cost_inr=300.0,
            probability=0.88, waitlisted=True,
        ),
        # --- Chennai -> Kumbakonam (direct, unreliable) -------------------
        TransitEdge(
            edge_id="tn_train_mas_kmu", origin=MAS, destination=KMU,
            mode="TRAIN", travel_time_min=330, cost_inr=610.0,
            probability=0.55, waitlisted=True,
        ),
    ]


# --------------------------------------------------------------------------- #
# Solver instances
# --------------------------------------------------------------------------- #
def tn_request(
    start: str = MAS,
    waypoints: Optional[List[str]] = None,
    start_time_min: int = 5 * 60,
    max_stops: int = 4,
    required: Optional[List[str]] = None,
) -> RoutingRequest:
    """A Tamil Nadu routing instance.

    ``waypoints`` is a soft preference (the solver picks the best circuit);
    ``required`` pins stops the plan *must* include.
    """
    return RoutingRequest(
        nodes=tn_nodes(),
        edges=tn_edges(),
        start_node_id=start,
        start_time_min=start_time_min,
        day_budget_min=17 * 60,
        max_stops=max_stops,
        cost_weight=1.0,
        risk_weight=5.0,
        required_node_ids=list(required or waypoints or []),
    )


def madurai_leg() -> Dict[str, object]:
    """The canonical Chennai -> Madurai rail leg used by the integration test."""
    return {
        "leg_id": "tn_vaigai_mas_mdu",
        "train_id": "tn_vaigai_mas_mdu",
        "origin": MAS,
        "destination": MDU,
        "travel_time_min": 540,
        "mode": "TRAIN",
        "waitlist_probability": 0.45,
    }


def rameswaram_leg() -> Dict[str, object]:
    """The Madurai -> Rameswaram leg that completes the headline journey."""
    return {
        "leg_id": "tn_bus_mdu_rmm",
        "train_id": "tn_bus_mdu_rmm",
        "origin": MDU,
        "destination": RMM,
        "travel_time_min": 210,
        "mode": "BUS",
        "waitlist_probability": 0.0,
    }


def headline_itinerary() -> List[Dict[str, object]]:
    """Chennai -> Madurai -> Rameswaram, the journey in the Mandate."""
    return [madurai_leg(), rameswaram_leg()]


# Node/edge metadata kept as plain dicts so the Cypher writer and the
# in-memory repository share one source of truth.
def nodes_as_rows() -> List[Dict[str, object]]:
    return [
        {
            "id": n.node_id,
            "name": n.name,
            "node_type": n.node_type.value,
            "reward": n.reward,
            "windows": [[w.start_min, w.end_min] for w in n.windows],
        }
        for n in tn_nodes()
    ]


def edges_as_rows() -> List[Dict[str, object]]:
    return [
        {
            "edge_id": e.edge_id,
            "origin": e.origin,
            "destination": e.destination,
            "mode": e.mode,
            "travel_time_min": e.travel_time_min,
            "cost_inr": e.cost_inr,
            "probability": e.probability,
            "waitlisted": e.waitlisted,
        }
        for e in tn_edges()
    ]


# --------------------------------------------------------------------------- #
# Seeding
# --------------------------------------------------------------------------- #
def seed_tamil_nadu(repository: Optional[Any] = None) -> Dict[str, int]:
    """Idempotently seed the Tamil Nadu topology.

    Safe to call repeatedly: the backend MERGEs on ``City.id`` and
    ``TRANSIT.edge_id``, so re-running only refreshes properties. Returns the
    resulting ``{"nodes": n, "edges": m}`` counts.
    """
    from .repository import get_repository

    repo = repository or get_repository()
    counts = repo.seed(nodes_as_rows(), edges_as_rows())
    log.info(
        "seeded Tamil Nadu topology via %s backend: %s",
        getattr(repo, "backend", "?"),
        counts,
    )
    return counts


def verify_seeded(repository: Optional[Any] = None) -> Dict[str, Any]:
    """Assert the mandatory hubs and their disconnected windows are present.

    Returns a report; raises :class:`AssertionError` if the graph does not
    satisfy the Mandate, which makes it usable directly as a test assertion.
    """
    from .repository import get_repository

    repo = repository or get_repository()
    nodes = {row["id"]: row for row in repo.all_nodes()}
    missing = [nid for nid in NODE_IDS if nid not in nodes]
    assert not missing, f"missing Tamil Nadu hubs: {missing}"

    # Meenakshi must carry BOTH sessions, i.e. genuinely disconnected.
    mdu = [(int(w[0]), int(w[1])) for w in (nodes[MDU].get("windows") or [])]
    assert mdu == MEENAKSHI_WINDOWS, (
        f"Meenakshi windows {mdu} != expected {MEENAKSHI_WINDOWS}"
    )

    edges = {row["edge_id"]: row for row in repo.all_edges()}
    for required_edge in ("tn_vaigai_mas_mdu", "tn_setc_mas_mdu"):
        assert required_edge in edges, f"missing edge {required_edge}"

    # The reliability trade-off the mandate calls out.
    vaigai = float(edges["tn_vaigai_mas_mdu"]["probability"])
    setc = float(edges["tn_setc_mas_mdu"]["probability"])
    assert vaigai < setc, "Vaigai Express must be less reliable than SETC"

    return {
        "backend": getattr(repo, "backend", "?"),
        "nodes": len(nodes),
        "edges": len(edges),
        "meenakshi_windows": mdu,
        "vaigai_p": vaigai,
        "setc_p": setc,
    }


def main() -> int:  # pragma: no cover - CLI entrypoint
    """``python -m app.graph.seed_tn`` -- seed and verify."""
    import json
    import logging

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    counts = seed_tamil_nadu()
    report = verify_seeded()
    print(json.dumps({"seeded": counts, "verified": report}, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

def nodes_as_rows() -> List[Dict[str, object]]:
    return [
        {
            "id": n.node_id,
            "name": n.name,
            "node_type": n.node_type.value,
            "reward": n.reward,
            "windows": [[w.start_min, w.end_min] for w in n.windows],
        }
        for n in tn_nodes()
    ]


def edges_as_rows() -> List[Dict[str, object]]:
    return [
        {
            "edge_id": e.edge_id,
            "origin": e.origin,
            "destination": e.destination,
            "mode": e.mode,
            "travel_time_min": e.travel_time_min,
            "cost_inr": e.cost_inr,
            "probability": e.probability,
            "waitlisted": e.waitlisted,
        }
        for e in tn_edges()
    ]

