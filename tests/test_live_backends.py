"""Opt-in verification against the REAL Memgraph and Redis backends.

The default suite runs on the in-memory repository and fakeredis so it needs no
containers. That leaves the production backends unexercised -- exactly where
bugs hide. Two real defects were found this way: orphaned methods that had
fallen out of their classes during editing, and a variable-length Cypher
pattern that Memgraph rejects (it makes ``r`` a list).

These tests therefore run only when the real services are reachable, and skip
cleanly otherwise:

    docker compose up -d redis memgraph
    pytest tests/test_live_backends.py -v

Override the endpoints with ``PAYANAM_LIVE_MEMGRAPH_URI`` /
``PAYANAM_LIVE_REDIS_URL``.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.fleet.matcher import (  # noqa: E402
    NoFleetAvailableError,
    claim_driver,
    dispatch_cab,
)
from app.fleet.state import FleetState  # noqa: E402
from app.graph.seed_tn import seed_tamil_nadu, verify_seeded  # noqa: E402

MEMGRAPH_URI = os.getenv("PAYANAM_LIVE_MEMGRAPH_URI", "bolt://localhost:7687")
REDIS_URL = os.getenv("PAYANAM_LIVE_REDIS_URL", "redis://localhost:6379/0")

CHENNAI = (80.2707, 13.0827)
DRIVERS = [
    ("drv-1", 80.2707, 13.0830),
    ("drv-2", 80.3000, 13.0900),
    ("drv-3", 80.3500, 13.1000),
    ("drv-4", 80.3900, 13.1200),
    ("drv-5", 80.6000, 13.4000),  # ~40 km: outside the 15 km radius
]


def _memgraph_or_skip():
    """Return a live MemgraphRepository, or skip if unreachable.

    Builds the client with an **explicit** ``Settings`` rather than mutating
    ``os.environ``. ``get_settings()`` is ``lru_cache``d, so an env override set
    after the first call is silently ignored -- which previously caused this
    helper to connect to whatever happened to be on the default port (7687) even
    when the caller had pointed ``PAYANAM_LIVE_MEMGRAPH_URI`` elsewhere.
    """
    from app.config import Settings
    from app.graph.repository import MemgraphRepository
    from app.graph.state import MemgraphClient

    settings = Settings(memgraph_uri=MEMGRAPH_URI)
    try:
        client = MemgraphClient(settings=settings)
        client.connect()
        client.driver.verify_connectivity()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"memgraph not reachable at {MEMGRAPH_URI}: {exc}")

    repo = MemgraphRepository(client=client)
    assert repo.backend == "memgraph"
    # Refuse to run destructive checks against anything we did not intend to
    # target: only a non-default, explicitly-configured instance is mutated.
    if MEMGRAPH_URI == "bolt://localhost:7687" and not os.getenv(
        "PAYANAM_LIVE_ALLOW_DEFAULT"
    ):
        pytest.skip(
            "refusing destructive checks on the default :7687 instance; set "
            "PAYANAM_LIVE_ALLOW_DEFAULT=1 to opt in"
        )
    return repo


async def _redis_or_skip() -> FleetState:
    """Return a FleetState on a real Redis, or skip."""
    import redis.asyncio as aioredis

    client = aioredis.from_url(REDIS_URL, decode_responses=False)
    try:
        await client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"redis not reachable at {REDIS_URL}: {exc}")
    assert "fakeredis" not in type(client).__module__, "expected a real client"
    return FleetState(client)
# --------------------------------------------------------------------------- #
# Memgraph
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_memgraph_seed_is_idempotent_and_verifiable() -> None:
    """Seed twice on a real server: counts must not drift."""
    repo = _memgraph_or_skip()
    repo.client.drop_transit_graph()
    assert repo.stats() == {"nodes": 0, "edges": 0}

    first = seed_tamil_nadu(repo)
    second = seed_tamil_nadu(repo)
    assert first == second == {"nodes": 5, "edges": 16}
    assert repo.stats() == {"nodes": 5, "edges": 16}

    report = verify_seeded(repo)
    assert report["backend"] == "memgraph"
    assert report["meenakshi_windows"] == [(300, 750), (960, 1320)]
    assert report["vaigai_p"] < report["setc_p"]

    repo.client.drop_transit_graph()


@pytest.mark.asyncio
async def test_memgraph_subgraph_matches_in_memory_backend() -> None:
    """Both backends must return the same edge set for the same hop count.

    Regression guard for the variable-length-pattern bug: Memgraph treats ``r``
    as a list under ``-[r:TRANSIT*1..d]->`` and rejects the property lookup, so
    expansion is single-hop + client-side BFS.
    """
    from app.graph.repository import InMemoryRepository

    repo = _memgraph_or_skip()
    repo.client.drop_transit_graph()
    seed_tamil_nadu(repo)

    memory = InMemoryRepository()
    seed_tamil_nadu(memory)

    for depth in (1, 2):
        live = {e["edge_id"] for e in repo.subgraph("MAS", depth)}
        fake = {e["edge_id"] for e in memory.subgraph("MAS", depth)}
        assert live == fake, f"depth {depth}: symmetric difference {live ^ fake}"
        assert live, f"depth {depth} returned nothing"

    repo.client.drop_transit_graph()


@pytest.mark.asyncio
async def test_memgraph_edge_multiplier_round_trip() -> None:
    """Live weight updates persist on the real server."""
    repo = _memgraph_or_skip()
    repo.client.drop_transit_graph()
    seed_tamil_nadu(repo)

    assert repo.apply_edge_multiplier("tn_vaigai_mas_mdu", 2.5) is True
    rows = repo.subgraph_rows("tn_vaigai_mas_mdu")
    assert rows and rows[0]["edge_id"] == "tn_vaigai_mas_mdu"

    repo.client.drop_transit_graph()


# --------------------------------------------------------------------------- #
# Redis
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_redis_geosearch_and_concurrent_locking() -> None:
    """The mandate's concurrency check against a real Redis server."""
    state = await _redis_or_skip()
    await state.clear()
    for did, lon, lat in DRIVERS:
        await state.update_driver_location(did, lon, lat, "AVAILABLE")
    assert await state.available_count() == 5

    found = await state.find_nearest_available(*CHENNAI, 15.0, 10)
    assert [c["driver_id"] for c in found] == ["drv-1", "drv-2", "drv-3", "drv-4"]
    distances = [c["distance_km"] for c in found]
    assert distances == sorted(distances)
    # Real Redis approximates per geohash cell, so allow slack.
    assert distances[0] < 1.0, distances[0]

    results = await asyncio.gather(
        dispatch_cab(*CHENNAI, "tourist-1", 15.0, state=state),
        dispatch_cab(*CHENNAI, "tourist-2", 15.0, state=state),
        return_exceptions=True,
    )
    assert not any(isinstance(r, Exception) for r in results), results
    claimed = [r["driver_id"] for r in results]
    assert len(set(claimed)) == 2, f"double-booked: {claimed}"
    assert set(claimed) == {"drv-1", "drv-2"}, claimed
    assert await state.locked_count() == 2

    await state.clear()


@pytest.mark.asyncio
async def test_redis_exhausted_fleet_is_terminal() -> None:
    """A drained fleet raises NoFleetAvailableError on a real server."""
    state = await _redis_or_skip()
    await state.clear()
    for did, lon, lat in DRIVERS[:4]:
        await state.update_driver_location(did, lon, lat, "AVAILABLE")

    while True:
        cands = await state.find_nearest_available(*CHENNAI, 15.0, 10)
        if not cands or await claim_driver(state, cands[0], "drain") is None:
            break

    with pytest.raises(NoFleetAvailableError):
        await dispatch_cab(*CHENNAI, "tourist-x", 15.0, state=state)

    await state.clear()