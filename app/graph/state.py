"""Memgraph connectivity and Cypher ingestion.

The official ``neo4j`` Python driver speaks Bolt, which Memgraph implements, so
it is the correct client for both.  Memgraph has no multi-database support,
hence the single ``memgraph`` database.

Phase 2 adds the *subgraph* accessor used by the route endpoint to pull routing
candidates for a hub; see :mod:`app.graph.repository` for the backend
abstraction that lets the same queries run against Memgraph or memory.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

from neo4j import AsyncGraphDatabase, GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable

from ..config import Settings, get_settings
from ..models import NodeType, TimeWindow, TransitEdge, TransitNode

log = logging.getLogger("payanam.graph")

# The canonical state-wide transit schema query (mandated shape).
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



class MemgraphClient:
    """Thin, retrying wrapper around the Memgraph Bolt endpoint."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self._driver = None

    # ------------------------------------------------------------- lifecycle
    def connect(self) -> None:
        if self._driver is not None:
            return
        log.info("connecting to memgraph at %s", self.settings.memgraph_uri)
        self._driver = GraphDatabase.driver(
            self.settings.memgraph_uri,
            auth=(
                (self.settings.memgraph_user, self.settings.memgraph_password)
                if self.settings.memgraph_user
                else None
            ),
            max_connection_pool_size=self.settings.memgraph_max_pool,
            connection_acquisition_timeout=15,
        )
        self._driver.verify_connectivity()
        log.info("memgraph connectivity verified")

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    @property
    def driver(self):
        if self._driver is None:
            self.connect()
        return self._driver

    def is_available(self) -> bool:
        try:
            self.driver.verify_connectivity()
            return True
        except (ServiceUnavailable, Neo4jError, OSError) as exc:
            log.warning("memgraph unavailable: %s", exc)
            return False

    # --------------------------------------------------------------- queries
    def run(self, query: str, **params: Any) -> List[Dict[str, Any]]:
        """Execute a read query and return a list of dicts."""
        with self.driver.session(database=self.settings.memgraph_database) as session:
            result = session.run(query, **params)
            return [record.data() for record in result]

    def execute(self, query: str, **params: Any) -> None:
        with self.driver.session(database=self.settings.memgraph_database) as session:
            session.run(query, **params).consume()

    def transit_schema(self) -> List[Dict[str, Any]]:
        """``MATCH (a:City)-[r:TRANSIT]->(b:City) RETURN r`` -- schema probe."""
        return self.run(TRANSIT_SCHEMA_QUERY)

    def transit_routes(self) -> List[Dict[str, Any]]:
        return self.run(TRANSIT_ROUTES_QUERY)

    # ------------------------------------------------- phase 2: enrichment
    def subgraph(self, origin: str, depth: int = 1) -> List[Dict[str, Any]]:
        """Outgoing :TRANSIT edges reachable from ``origin`` within ``depth``.

        This is the routing-candidate query: the API uses it to build a
        :class:`~app.models.RoutingRequest` from live topology instead of a
        hard-coded fixture.
        """
        from .repository import subgraph_query

        return self.run(subgraph_query(depth), origin=origin)

    def routing_candidates(self, origin: str, depth: int = 1):
        """``(nodes, edges)`` in solver-ready domain objects."""
        from .repository import rows_to_edges, rows_to_nodes

        edge_rows = self.subgraph(origin, depth)
        node_ids = {origin} | {r["destination"] for r in edge_rows}
        all_nodes = {
            r["id"]: r
            for r in self.run(
                "MATCH (c:City) WHERE c.id IN $ids RETURN c.id AS id, "
                "c.name AS name, c.node_type AS node_type, c.reward AS reward, "
                "c.windows AS windows",
                ids=list(node_ids),
            )
        }
        return (
            rows_to_nodes([all_nodes[i] for i in node_ids if i in all_nodes]),
            rows_to_edges(edge_rows),
        )

    # ------------------------------------------------------------ ingestion
    def upsert_nodes(self, nodes: Iterable[TransitNode]) -> int:
        cypher = """
        UNWIND $rows AS row
        MERGE (c:City {id: row.id})
          ON CREATE SET c.created_at = timestamp()
        SET c.name     = row.name,
            c.node_type = row.node_type,
            c.windows  = row.windows
        """
        rows = [
            {
                "id": nd.node_id,
                "name": nd.name,
                "node_type": nd.node_type.value,
                # persist the disconnected windows so the solver reads them back
                "windows": [[w.start_min, w.end_min] for w in nd.windows],
            }
            for nd in nodes
        ]
        if not rows:
            return 0
        self.execute(cypher, rows=rows)
        return len(rows)

    def upsert_edges(self, edges: Iterable[TransitEdge]) -> int:
        cypher = """
        UNWIND $rows AS row
        MATCH (a:City {id: row.origin})
        MATCH (b:City {id: row.destination})
        MERGE (a)-[r:TRANSIT {edge_id: row.edge_id}]->(b)
        SET r.mode            = row.mode,
            r.travel_time_min = row.travel_time_min,
            r.cost_inr        = row.cost_inr,
            r.probability     = row.probability,
            r.waitlisted      = row.waitlisted,
            r.multiplier      = 1.0,
            r.updated_at      = timestamp()
        """
        rows = [
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
            for e in edges
        ]
        if not rows:
            return 0
        self.execute(cypher, rows=rows)
        return len(rows)

    def apply_edge_multiplier(self, edge_id: str, multiplier: float) -> None:
        """Persist a live edge-weight adjustment produced by the Kafka stream."""
        self.execute(
            "MATCH ()-[r:TRANSIT {edge_id: $edge_id}]->() "
            "SET r.multiplier = $multiplier, r.updated_at = timestamp()",
            edge_id=edge_id,
            multiplier=float(multiplier),
        )

    def to_domain(self) -> Tuple[List[TransitNode], List[TransitEdge]]:
        """Materialise the graph into solver-ready domain objects."""
        nodes: List[TransitNode] = []
        edges: List[TransitEdge] = []
        for row in self.run(
            "MATCH (c:City) RETURN c.id AS id, c.name AS name, "
            "c.node_type AS node_type, c.windows AS windows"
        ):
            windows = [
                TimeWindow(start_min=int(a), end_min=int(b))
                for a, b in (row.get("windows") or [])
            ]
            try:
                node_type = NodeType(row.get("node_type") or "WAYPOINT")
            except ValueError:
                node_type = NodeType.WAYPOINT
            nodes.append(
                TransitNode(
                    node_id=row["id"],
                    name=row.get("name") or row["id"],
                    node_type=node_type,
                    windows=windows,
                )
            )
        for row in self.run(TRANSIT_ROUTES_QUERY):
            edges.append(
                TransitEdge(
                    edge_id=row["edge_id"],
                    origin=row["origin"],
                    destination=row["destination"],
                    mode=row.get("mode") or "TRAIN",
                    travel_time_min=int(row["travel_time_min"]),
                    cost_inr=float(row.get("cost_inr") or 0.0),
                    probability=float(row.get("probability") or 1.0),
                    waitlisted=bool(row.get("waitlisted")),
                )
            )
        return nodes, edges



# --------------------------------------------------------------------------- #
# Async facade (used by the FastAPI lifespan and by activities)
# --------------------------------------------------------------------------- #
class AsyncMemgraphClient:
    """``AsyncGraphDatabase`` wrapper so activities never block the event loop."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self._driver = None

    async def connect(self) -> None:
        if self._driver is not None:
            return
        self._driver = AsyncGraphDatabase.driver(
            self.settings.memgraph_uri,
            auth=(
                (self.settings.memgraph_user, self.settings.memgraph_password)
                if self.settings.memgraph_user
                else None
            ),
            max_connection_pool_size=self.settings.memgraph_max_pool,
        )
        await self._driver.verify_connectivity()
        log.info("async memgraph driver ready at %s", self.settings.memgraph_uri)

    async def close(self) -> None:
        if self._driver is not None:
            await self._driver.close()
            self._driver = None

    async def run(self, query: str, **params: Any) -> List[Dict[str, Any]]:
        if self._driver is None:
            await self.connect()
        async with self._driver.session(
            database=self.settings.memgraph_database
        ) as session:
            result = await session.run(query, **params)
            return [record.data() async for record in result]

    async def transit_schema(self) -> List[Dict[str, Any]]:
        return await self.run(TRANSIT_SCHEMA_QUERY)

    async def apply_edge_multiplier(self, edge_id: str, multiplier: float) -> None:
        await self.run(
            "MATCH ()-[r:TRANSIT {edge_id: $edge_id}]->() "
            "SET r.multiplier = $multiplier, r.updated_at = timestamp()",
            edge_id=edge_id,
            multiplier=float(multiplier),
        )

    async def seed_demo_state(self) -> Dict[str, int]:
        """Idempotently seed a small South-Indian circuit for the sandbox."""
        from ..sample_data import sample_edges, sample_nodes

        nodes, edges = sample_nodes(), sample_edges()
        if self._driver is None:
            await self.connect()
        async with self._driver.session(
            database=self.settings.memgraph_database
        ) as session:
            await session.run(
                "UNWIND $rows AS row MERGE (c:City {id: row.id}) "
                "SET c.name = row.name, c.node_type = row.node_type, "
                "c.windows = row.windows",
                rows=[
                    {
                        "id": n.node_id,
                        "name": n.name,
                        "node_type": n.node_type.value,
                        "windows": [[w.start_min, w.end_min] for w in n.windows],
                    }
                    for n in nodes
                ],
            )
            await session.run(
                "UNWIND $rows AS row "
                "MATCH (a:City {id: row.origin}) MATCH (b:City {id: row.destination}) "
                "MERGE (a)-[r:TRANSIT {edge_id: row.edge_id}]->(b) "
                "SET r.mode = row.mode, r.travel_time_min = row.travel_time_min, "
                "r.cost_inr = row.cost_inr, r.probability = row.probability, "
                "r.waitlisted = row.waitlisted, r.multiplier = 1.0",
                rows=[
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
                    for e in edges
                ],
            )
        return {"nodes": len(nodes), "edges": len(edges)}


async def retry_connect(
    client: AsyncMemgraphClient, attempts: int = 5, delay: float = 2.0
) -> bool:
    """Best-effort startup: Memgraph may still be booting in Compose."""
    last: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            await client.connect()
            return True
        except Exception as exc:  # noqa: BLE001 - retried, then swallowed
            last = exc
            log.warning(
                "memgraph connect attempt %s/%s failed: %s", attempt, attempts, exc
            )
            await asyncio.sleep(delay)
    log.error("giving up on memgraph: %s", last)
    return False

    # ---------------------------------------------------------------- admin
    def drop_transit_graph(self) -> None:
        self.execute("MATCH (n) DETACH DELETE n")

    def stats(self) -> Dict[str, int]:
        rows = self.run(
            "MATCH (c:City) WITH count(c) AS cities "
            "MATCH ()-[r:TRANSIT]->() RETURN cities, count(r) AS edges"
        )
        if not rows:
            return {"cities": 0, "edges": 0}
        return {
            "cities": int(rows[0].get("cities") or 0),
            "edges": int(rows[0].get("edges") or 0),
        }

