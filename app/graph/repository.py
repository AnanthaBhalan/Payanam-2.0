"""Enriched graph access: subgraph candidates for routing + seeding.

Two interchangeable backends implement the same surface:

* :class:`MemgraphRepository` -- Cypher over Bolt (the production path).
* :class:`InMemoryRepository` -- an in-process graph with identical semantics,
  used by the integration test so the suite is runnable without Docker.

``get_repository()`` prefers Memgraph and falls back to memory, so the same
seeding and query code exercises either backend.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence

from ..models import NodeType, TimeWindow, TransitEdge, TransitNode

log = logging.getLogger("payanam.graph")

# The canonical state-wide transit schema query (Phase 1 contract, retained).
TRANSIT_SCHEMA_QUERY = """
MATCH (a:City)-[r:TRANSIT]->(b:City)
RETURN r
ORDER BY r.travel_time_min ASC
"""

TRANSIT_ROUTES_QUERY = """
MATCH (a:City)-[r:TRANSIT]->(b:City)
RETURN a.id AS origin,
       b.id AS destination,
       r.edge_id AS edge_id,
       r.mode AS mode,
       r.travel_time_min AS travel_time_min,
       r.cost_inr AS cost_inr,
       r.probability AS probability,
       r.waitlisted AS waitlisted
"""

# Enrichment (Phase 2): the subgraph of edges reachable from `origin` within
# `depth` hops, carrying node metadata so the solver can apply windows.
SUBGRAPH_QUERY = """
MATCH (a:City {id: $origin})-[r:TRANSIT*1..%d]->(b:City)
WITH a, r, b
RETURN a.id AS origin,
       b.id AS destination,
       r.edge_id AS edge_id,
       r.mode AS mode,
       r.travel_time_min AS travel_time_min,
       r.cost_inr AS cost_inr,
       r.probability AS probability,
       r.waitlisted AS waitlisted
"""


def subgraph_query(depth: int = 1) -> str:
    """Cypher for the outgoing edge subgraph within ``depth`` hops."""
    return SUBGRAPH_QUERY % max(1, min(int(depth), 4))


# --------------------------------------------------------------------------- #
# Repository contract
# --------------------------------------------------------------------------- #
class TransitRepository:
    """Minimal contract shared by both backends."""

    backend: str = "abstract"

    # -- writes ------------------------------------------------------------
    def seed(
        self,
        nodes: Iterable[Dict[str, Any]],
        edges: Iterable[Dict[str, Any]],
    ) -> Dict[str, int]:  # pragma: no cover - interface
        raise NotImplementedError

    def apply_edge_multiplier(self, edge_id: str, multiplier: float) -> bool:
        raise NotImplementedError  # pragma: no cover - interface

    # -- reads -------------------------------------------------------------
    def all_nodes(self) -> List[Dict[str, Any]]:  # pragma: no cover - interface
        raise NotImplementedError

    def all_edges(self) -> List[Dict[str, Any]]:  # pragma: no cover - interface
        raise NotImplementedError

    def subgraph(self, origin: str, depth: int = 1) -> List[Dict[str, Any]]:
        raise NotImplementedError  # pragma: no cover - interface

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    def stats(self) -> Dict[str, int]:  # pragma: no cover - interface
        raise NotImplementedError


# --------------------------------------------------------------------------- #
# In-memory backend
# --------------------------------------------------------------------------- #
class InMemoryRepository(TransitRepository):
    """In-process graph with Memgraph-equivalent semantics.

    ``seed`` is idempotent via MERGE-style upsert on ``id``/``edge_id``, so
    running the seeder twice leaves the graph unchanged -- the same guarantee
    the Cypher ``MERGE`` gives.
    """

    backend = "memory"

    def __init__(self) -> None:
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._edges: Dict[str, Dict[str, Any]] = {}

    # -- writes ------------------------------------------------------------
    def seed(
        self,
        nodes: Iterable[Dict[str, Any]],
        edges: Iterable[Dict[str, Any]],
    ) -> Dict[str, int]:
        # MERGE (c:City {id: ...}) then SET -- idempotent by construction.
        for row in nodes:
            existing = self._nodes.get(row["id"], {})
            merged = {**existing, **row}
            merged.setdefault("multiplier", 1.0)
            self._nodes[row["id"]] = merged
        for row in edges:
            existing = self._edges.get(row["edge_id"], {})
            merged = {**existing, **row}
            merged.setdefault("multiplier", 1.0)
            self._edges[row["edge_id"]] = merged
        return {"nodes": len(self._nodes), "edges": len(self._edges)}

    def apply_edge_multiplier(self, edge_id: str, multiplier: float) -> bool:
        edge = self._edges.get(edge_id)
        if edge is None:
            return False
        edge["multiplier"] = float(multiplier)
        return True

    # -- reads -------------------------------------------------------------
    def all_nodes(self) -> List[Dict[str, Any]]:
        return [dict(v) for v in self._nodes.values()]

    def all_edges(self) -> List[Dict[str, Any]]:
        return [dict(v) for v in self._edges.values()]

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        row = self._nodes.get(node_id)
        return dict(row) if row else None

    def subgraph(self, origin: str, depth: int = 1) -> List[Dict[str, Any]]:
        """Breadth-first expansion, mirroring ``-[r:TRANSIT*1..d]->``."""
        frontier = {origin}
        seen_nodes = {origin}
        out: List[Dict[str, Any]] = []
        seen_edges: set = set()
        for _ in range(max(1, min(int(depth), 4))):
            next_frontier: set = set()
            for edge in self._edges.values():
                if edge["origin"] not in frontier:
                    continue
                if edge["edge_id"] in seen_edges:
                    continue
                seen_edges.add(edge["edge_id"])
                out.append(dict(edge))
                nxt = edge["destination"]
                if nxt not in seen_nodes:
                    seen_nodes.add(nxt)
                    next_frontier.add(nxt)
            frontier = next_frontier
            if not frontier:
                break
        return out

    def clear(self) -> None:
        self._nodes.clear()
        self._edges.clear()

    def stats(self) -> Dict[str, int]:
        return {"nodes": len(self._nodes), "edges": len(self._edges)}


# --------------------------------------------------------------------------- #
# Memgraph backend
# --------------------------------------------------------------------------- #
class MemgraphRepository(TransitRepository):
    """Cypher over Bolt. Memgraph has no multi-database support."""

    backend = "memgraph"

    def __init__(self, client: Optional[Any] = None) -> None:
        # Imported lazily so the module stays usable with no neo4j installed.
        if client is None:
            from .state import MemgraphClient

            client = MemgraphClient()
            client.connect()
        self.client = client

    # -- writes ------------------------------------------------------------
    def seed(
        self,
        nodes: Iterable[Dict[str, Any]],
        edges: Iterable[Dict[str, Any]],
    ) -> Dict[str, int]:
        node_rows = list(nodes)
        edge_rows = list(edges)
        self.client.execute(
            """
            UNWIND $rows AS row
            MERGE (c:City {id: row.id})
              ON CREATE SET c.created_at = timestamp()
            SET c.name     = row.name,
                c.node_type = row.node_type,
                c.reward    = row.reward,
                c.windows   = row.windows
            """,
            rows=node_rows,
        )
        self.client.execute(
            """
            UNWIND $rows AS row
            MATCH (a:City {id: row.origin})
            MATCH (b:City {id: row.destination})
            MERGE (a)-[r:TRANSIT {edge_id: row.edge_id}]->(b)
            SET r.mode            = row.mode,
                r.travel_time_min = row.travel_time_min,
                r.cost_inr        = row.cost_inr,
                r.probability     = row.probability,
                r.waitlisted      = row.waitlisted,
                r.multiplier      = coalesce(r.multiplier, 1.0),
                r.updated_at      = timestamp()
            """,
            rows=edge_rows,
        )
        return {"nodes": len(node_rows), "edges": len(edge_rows)}

    def apply_edge_multiplier(self, edge_id: str, multiplier: float) -> bool:
        before = len(self.subgraph_rows(edge_id))
        self.client.apply_edge_multiplier(edge_id, multiplier)
        return before >= 0

    # -- reads -------------------------------------------------------------
    def all_nodes(self) -> List[Dict[str, Any]]:
        return self.client.run(
            "MATCH (c:City) RETURN c.id AS id, c.name AS name, "
            "c.node_type AS node_type, c.reward AS reward, c.windows AS windows"
        )

    def all_edges(self) -> List[Dict[str, Any]]:
        return self.client.run(TRANSIT_ROUTES_QUERY)

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        rows = self.client.run(
            "MATCH (c:City {id: $node_id}) RETURN c.id AS id, c.name AS name, "
            "c.node_type AS node_type, c.reward AS reward, c.windows AS windows",
            node_id=node_id,
        )
        return rows[0] if rows else None

# --------------------------------------------------------------------------- #
# Row <-> domain mapping
# --------------------------------------------------------------------------- #
def rows_to_nodes(rows: Sequence[Dict[str, Any]]) -> List[TransitNode]:
    """Materialise node rows into :class:`TransitNode` objects."""
    out: List[TransitNode] = []
    for row in rows:
        windows = []
        for pair in row.get("windows") or []:
            start, end = int(pair[0]), int(pair[1])
            try:
                windows.append(TimeWindow(start_min=start, end_min=end))
            except ValueError:
                continue  # tolerate a malformed window rather than failing
        try:
            node_type = NodeType(row.get("node_type") or "WAYPOINT")
        except ValueError:
            node_type = NodeType.WAYPOINT
        out.append(
            TransitNode(
                node_id=row["id"],
                name=row.get("name") or row["id"],
                node_type=node_type,
                windows=windows,
                reward=float(row.get("reward") or 0.0),
            )
        )
    return out


def rows_to_edges(rows: Sequence[Dict[str, Any]]) -> List[TransitEdge]:
    """Materialise edge rows into :class:`TransitEdge` objects."""
    out: List[TransitEdge] = []
    for row in rows:
        if row.get("origin") is None or row.get("destination") is None:
            continue  # dangling relation
        out.append(
            TransitEdge(
                edge_id=row["edge_id"],
                origin=row["origin"],
                destination=row["destination"],
                mode=row.get("mode") or "TRAIN",
                travel_time_min=int(row.get("travel_time_min") or 1),
                cost_inr=float(row.get("cost_inr") or 0.0),
                probability=float(row.get("probability") or 1.0),
                waitlisted=bool(row.get("waitlisted")),
            )
        )
    return out


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
_REPO: Optional[TransitRepository] = None


def get_repository(
    force_memory: bool = False, refresh: bool = False
) -> TransitRepository:
    """Return the active repository, preferring Memgraph when reachable.

    Falls back to :class:`InMemoryRepository` so the API, seeder and tests all
    work with or without a running Memgraph.
    """
    global _REPO
    if _REPO is not None and not refresh and not force_memory:
        return _REPO

    if force_memory:
        _REPO = InMemoryRepository()
        return _REPO

    try:
        repo = MemgraphRepository()
        stats = repo.stats()
        log.info(
            "using memgraph repository (%s nodes, %s edges)", *stats.values()
        )
        _REPO = repo
    except Exception as exc:  # noqa: BLE001 - degrade to memory
        log.warning("memgraph unavailable (%s); using in-memory graph", exc)
        _REPO = InMemoryRepository()
    return _REPO


def set_repository(repo: Optional[TransitRepository]) -> None:
    """Override the active repository (used by tests and the seeder)."""
    global _REPO
    _REPO = repo

    def subgraph(self, origin: str, depth: int = 1) -> List[Dict[str, Any]]:
        return self.client.run(subgraph_query(depth), origin=origin)

    def subgraph_rows(self, edge_id: str) -> List[Dict[str, Any]]:
        return self.client.run(
            "MATCH ()-[r:TRANSIT {edge_id: $edge_id}]->() RETURN r.edge_id AS edge_id",
            edge_id=edge_id,
        )

    def stats(self) -> Dict[str, int]:
        s = self.client.stats()
        return {"nodes": int(s.get("cities", 0)), "edges": int(s.get("edges", 0))}

        return out

    def stats(self) -> Dict[str, int]:
        return {"nodes": len(self._nodes), "edges": len(self._edges)}

    def clear(self) -> None:
        self._nodes.clear()
        self._edges.clear()

