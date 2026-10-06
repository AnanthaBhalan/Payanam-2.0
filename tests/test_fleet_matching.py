"""Phase 3: real-time fleet matching -- spatial queries and atomic locking.

Run with::

    python -m pytest tests/test_fleet_matching.py -v

Redis is provided by ``fakeredis`` (same GEO/WATCH/SET-NX semantics as a real
server), so the suite needs no containers.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Dict, List

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fakeredis.aioredis as fakeaioredis  # noqa: E402

from app.fleet.matcher import (  # noqa: E402
    NoFleetAvailableError,
    claim_driver,
    dispatch_cab,
    match_batch,
)
from app.fleet.state import ASSIGNED, AVAILABLE, FleetState  # noqa: E402
from app.graph.seed_tn import hub_coords  # noqa: E402

# Chennai Central -- the Mandate's reference pickup point.
CHENNAI = hub_coords("MAS")
CHENNAI_LON, CHENNAI_LAT = CHENNAI
assert (round(CHENNAI_LON, 4), round(CHENNAI_LAT, 4)) == (80.2707, 13.0827)

# Five drivers at increasing distance from Chennai Central, so the expected
# dispatch order is deterministic.
FLEET: List[Dict[str, object]] = [
    # ~0.08 km
    {"driver_id": "drv-1", "lon": 80.2707, "lat": 13.0830},
    # ~3.3 km
    {"driver_id": "drv-2", "lon": 80.3000, "lat": 13.0900},
    # ~8.8 km
    {"driver_id": "drv-3", "lon": 80.3500, "lat": 13.1000},
    # ~12 km
    {"driver_id": "drv-4", "lon": 80.3900, "lat": 13.1200},
    # ~40 km -- outside the 15 km dispatch radius
    {"driver_id": "drv-5", "lon": 80.6000, "lat": 13.4000},
]

EXPECTED_ORDER = ["drv-1", "drv-2", "drv-3", "drv-4"]


@pytest.fixture
async def fleet() -> FleetState:
    """A fakeredis-backed fleet seeded with the five drivers."""
    redis = fakeaioredis.FakeRedis()
    state = FleetState(redis)
    for driver in FLEET:
        await state.update_driver_location(
            str(driver["driver_id"]),
            float(driver["lon"]),
            float(driver["lat"]),
            AVAILABLE,
        )
    return state

# --------------------------------------------------------------------------- #
# 1. Spatial queries
# --------------------------------------------------------------------------- #
async def test_geosearch_returns_drivers_nearest_first(fleet: FleetState) -> None:
    """GEOSEARCH orders by real great-circle distance, ascending."""
    found = await fleet.find_nearest_available(
        CHENNAI_LON, CHENNAI_LAT, radius_km=15.0, limit=10
    )
    ids = [c["driver_id"] for c in found]
    assert ids == EXPECTED_ORDER, f"expected {EXPECTED_ORDER}, got {ids}"

    distances = [c["distance_km"] for c in found]
    assert distances == sorted(distances), f"not sorted: {distances}"
    assert all(d <= 15.0 for d in distances), distances
    assert distances[0] < 0.5, distances[0]
    assert 5.0 < distances[-1] < 15.0, distances[-1]


async def test_radius_excludes_out_of_range_drivers(fleet: FleetState) -> None:
    """drv-5 sits ~40km out and must never appear in a 15km search."""
    found = await fleet.find_nearest_available(
        CHENNAI_LON, CHENNAI_LAT, radius_km=15.0, limit=10
    )
    assert "drv-5" not in {c["driver_id"] for c in found}

    wide = await fleet.find_nearest_available(
        CHENNAI_LON, CHENNAI_LAT, radius_km=100.0, limit=10
    )
    assert "drv-5" in {c["driver_id"] for c in wide}


async def test_offline_driver_is_not_dispatchable(fleet: FleetState) -> None:
    """A driver who logs OFFLINE leaves the available index."""
    await fleet.set_status("drv-1", "OFFLINE")
    found = await fleet.find_nearest_available(
        CHENNAI_LON, CHENNAI_LAT, radius_km=15.0, limit=10
    )
    assert "drv-1" not in {c["driver_id"] for c in found}
    assert [c["driver_id"] for c in found] == EXPECTED_ORDER[1:]

    await fleet.set_status("drv-1", AVAILABLE)
    found = await fleet.find_nearest_available(
        CHENNAI_LON, CHENNAI_LAT, radius_km=15.0, limit=10
    )
    assert found[0]["driver_id"] == "drv-1"


async def test_dispatch_claims_closest_and_removes_it(fleet: FleetState) -> None:
    """A single dispatch takes the nearest cab out of the pool."""
    assignment = await dispatch_cab(
        CHENNAI_LON, CHENNAI_LAT, "tourist-A", radius_km=15.0, state=fleet
    )
    assert assignment["driver_id"] == "drv-1"
    assert assignment["status"] == ASSIGNED
    assert assignment["assigned_to"] == "tourist-A"
    assert assignment["dispatch_id"]

    driver = await fleet.get_driver("drv-1")
    assert driver["status"] == ASSIGNED
    assert driver["assigned_to"] == "tourist-A"

    remaining = await fleet.find_nearest_available(
        CHENNAI_LON, CHENNAI_LAT, radius_km=15.0, limit=10
    )
    assert "drv-1" not in {c["driver_id"] for c in remaining}


# --------------------------------------------------------------------------- #
# 2. Concurrency: the core mandate
# --------------------------------------------------------------------------- #
async def test_concurrent_dispatch_never_double_books_one_driver(
    fleet: FleetState,
) -> None:
    """Two simultaneous dispatches settle on the two closest drivers.

    This is the TOCTOU the WATCH/lease transaction exists to prevent: without
    it both coroutines read drv-1 as nearest and both would claim it.
    """
    results = await asyncio.gather(
        dispatch_cab(CHENNAI_LON, CHENNAI_LAT, "tourist-1", 15.0, state=fleet),
        dispatch_cab(CHENNAI_LON, CHENNAI_LAT, "tourist-2", 15.0, state=fleet),
        return_exceptions=True,
    )

    # Neither may fail: a race degrades to "next nearest", never to an error.
    assert not any(isinstance(r, Exception) for r in results), results

    claimed = [r["driver_id"] for r in results]
    assert len(set(claimed)) == 2, f"double-booked: {claimed}"
    assert set(claimed) == {"drv-1", "drv-2"}, claimed

    # Each driver's owner matches the assignment it was given.
    by_tourist = {r["assigned_to"]: r for r in results}
    assert set(by_tourist) == {"tourist-1", "tourist-2"}
    for tourist_id, result in by_tourist.items():
        driver = await fleet.get_driver(result["driver_id"])
        assert driver["assigned_to"] == tourist_id

    # The closer cab goes to whichever request resolved it first; either way
    # drv-1 is never further from the pickup than drv-2.
    d1 = by_tourist["tourist-1"]["distance_km"]
    d2 = by_tourist["tourist-2"]["distance_km"]
    closest = min(d1, d2)
    assert closest < 0.5, (d1, d2)


async def test_many_concurrent_dispatches_exhaust_pool_exactly_once(
    fleet: FleetState,
) -> None:
    """With more demand than supply, every cab is handed out at most once."""
    outcomes = await asyncio.gather(
        *(dispatch_cab(CHENNAI_LON, CHENNAI_LAT, f"tourist-{i}", 15.0, state=fleet)
          for i in range(8)),
        return_exceptions=True,
    )
    ok = [r for r in outcomes if isinstance(r, dict)]
    failed = [r for r in outcomes if isinstance(r, NoFleetAvailableError)]

    claimed = [r["driver_id"] for r in ok]
    assert len(claimed) == len(set(claimed)), f"double-booked: {claimed}"
    assert set(claimed) == set(EXPECTED_ORDER), claimed
    assert len(failed) == 4, [type(r).__name__ for r in outcomes]
    # drv-5 remains AVAILABLE (it was never in radius, so it was never claimed).
    assert await fleet.available_count() == 1


async def test_claim_is_not_reentrant(fleet: FleetState) -> None:
    """A direct double-claim on one driver: the second must lose."""
    driver = (
        await fleet.find_nearest_available(CHENNAI_LON, CHENNAI_LAT, 15.0, 10)
    )[0]
    first = await claim_driver(fleet, driver, "tourist-1")
    second = await claim_driver(fleet, driver, "tourist-2")
    assert first is not None
    assert second is None, "a driver must not be claimable twice"


async def test_release_returns_driver_to_pool(fleet: FleetState) -> None:
    """A released cab becomes dispatchable again (abort/timeout recovery)."""
    # 5 drivers seeded as AVAILABLE; drv-5 is outside radius but still counted.
    assert await fleet.available_count() == 5

    await dispatch_cab(CHENNAI_LON, CHENNAI_LAT, "tourist-A", 15.0, state=fleet)
    assert await fleet.available_count() == 4  # drv-1 moved to the locked set

    await fleet.release_driver("drv-1")
    assert await fleet.available_count() == 5
    found = await fleet.find_nearest_available(
        CHENNAI_LON, CHENNAI_LAT, 15.0, 10
    )
    assert found[0]["driver_id"] == "drv-1"


async def test_no_fleet_available_is_terminal(fleet: FleetState) -> None:
    """A pickup far from any cab raises the typed terminal error."""
    with pytest.raises(NoFleetAvailableError) as excinfo:
        await dispatch_cab(78.0, 9.0, "stranded-tourist", 15.0, state=fleet)
    assert excinfo.value.code == "NO_FLEET_AVAILABLE"


async def test_match_batch_assigns_distinct_drivers(fleet: FleetState) -> None:
    """The bipartite form matches many tourists to distinct drivers."""
    result = await match_batch(
        [
            {"tourist_id": "t-1", "lon": CHENNAI_LON, "lat": CHENNAI_LAT},
            {"tourist_id": "t-2", "lon": CHENNAI_LON, "lat": CHENNAI_LAT},
            {"tourist_id": "t-3", "lon": CHENNAI_LON, "lat": CHENNAI_LAT},
        ],
        radius_km=15.0,
        state=fleet,
    )
    assignments = result["assignments"]
    assert len(assignments) == 3, result
    drivers = [a["driver_id"] for a in assignments.values()]
    assert len(set(drivers)) == 3, f"double-booked: {drivers}"
    assert set(drivers) == {"drv-1", "drv-2", "drv-3"}
async def test_book_cab_activity_dispatches_and_claims(fleet: FleetState) -> None:
    """The Temporal activity resolves a hub to coordinates and claims a cab.

    ``@activity.defn`` leaves the coroutine callable directly, so the body can
    be exercised without a worker -- which is exactly what we want to assert:
    the hub code is geolocated and a real driver is claimed.
    """
    from app.fleet.state import set_fleet_state
    from app.workflows.activities import book_cab

    set_fleet_state(fleet)
    try:
        receipt = await book_cab("itin-book-1", "MAS", "MDU", "waitlist_dropped")
    finally:
        set_fleet_state(None)

    assert receipt["kind"] == "CAB"
    # "MAS" -> Chennai Central, exactly as the Mandate pins it.
    assert receipt["pickup_lon"] == pytest.approx(80.2707)
    assert receipt["pickup_lat"] == pytest.approx(13.0827)
    assert receipt["driver_id"] == "drv-1", receipt

    # The driver is genuinely locked, not merely reported as such.
    driver = await fleet.get_driver("drv-1")
    assert driver["status"] == ASSIGNED


async def test_no_fleet_available_surfaces_as_application_error(
    fleet: FleetState,
) -> None:
    """An exhausted fleet becomes the terminal NO_FLEET_AVAILABLE error."""
    from temporalio.exceptions import ApplicationError

    from app.fleet.state import set_fleet_state
    from app.workflows.activities import book_cab

    # Drain every in-radius cab so the next dispatch has nothing to claim.
    while True:
        candidates = await fleet.find_nearest_available(
            CHENNAI_LON, CHENNAI_LAT, 15.0, 10
        )
        if not candidates:
            break
        if await claim_driver(fleet, candidates[0], f"seed-{candidates[0]['driver_id']}") is None:
            break

    set_fleet_state(fleet)
    try:
        with pytest.raises(ApplicationError) as excinfo:
            await book_cab("itin-book-2", "MAS", "MDU", "waitlist_dropped")
    finally:
        set_fleet_state(None)

    assert "NO_FLEET_AVAILABLE" in str(excinfo.value)
    assert excinfo.value.non_retryable is True


async def test_unknown_hub_is_terminal(fleet: FleetState) -> None:
    """An ungeolocatable hub fails fast rather than dispatching to nowhere."""
    from temporalio.exceptions import ApplicationError

    from app.fleet.state import set_fleet_state
    from app.workflows.activities import book_cab

    set_fleet_state(fleet)
    try:
        with pytest.raises(ApplicationError) as excinfo:
            await book_cab("itin-book-3", "XXX", "MDU", "waitlist_dropped")
    finally:
        set_fleet_state(None)


# --------------------------------------------------------------------------- #
# 6. Phase 11: leases, release-on-completion and self-healing reclaim
# --------------------------------------------------------------------------- #
async def test_release_cab_activity_is_registered(fleet: FleetState) -> None:
    """Temporal raises ActivityNotRegistered at trip end otherwise.

    The workflow now calls ``release_cab`` before completing; if the activity
    is missing from ``ACTIVITIES`` the release fails silently and the driver
    stays locked until its TTL fires -- exactly the leak Phase 11 fixes.
    """
    from app.workflows.activities import ACTIVITIES

    assert "release_cab" in {a.__name__ for a in ACTIVITIES}


async def test_reclaim_expired_locks_restores_orphaned_driver(
    fleet: FleetState,
) -> None:
    """A lease that dies without ``release_cab`` must self-heal (Phase 11)."""
    assignment = await dispatch_cab(
        CHENNAI_LON, CHENNAI_LAT, "tourist-A", 15.0, state=fleet
    )
    did = assignment["driver_id"]
    assert await fleet.locked_count() == 1

    # Simulate TTL expiry: the lease key is gone but the driver hash and GEO
    # index still say ASSIGNED -- the exact orphan state Phase 10 leaked into.
    await fleet.redis.delete(fleet.lock_key(did))
    assert await fleet.reclaim_expired_locks() == 1

    driver = await fleet.get_driver(did)
    assert driver["status"] == AVAILABLE
    assert await fleet.available_count() == 5
    assert await fleet.locked_count() == 0

    # A second pass finds nothing to do (idempotent janitor).
    assert await fleet.reclaim_expired_locks() == 0


async def test_live_lease_is_not_reclaimed(fleet: FleetState) -> None:
    """The reclaim pass must never steal a genuinely held driver."""
    assignment = await dispatch_cab(
        CHENNAI_LON, CHENNAI_LAT, "tourist-A", 15.0, state=fleet
    )
    did = assignment["driver_id"]
    assert await fleet.redis.exists(fleet.lock_key(did))

    assert await fleet.reclaim_expired_locks() == 0
    driver = await fleet.get_driver(did)
    assert driver["status"] == ASSIGNED
    assert await fleet.locked_count() == 1


async def test_available_ping_clears_stale_lease(fleet: FleetState) -> None:
    """A driver reporting AVAILABLE must be immediately dispatchable.

    Without this, a leftover ``SET NX`` lease poisons the driver: every
    future claim loses on the lease until the old TTL fires, so re-seeding
    the fleet could not repair a leaked run.
    """
    assignment = await dispatch_cab(
        CHENNAI_LON, CHENNAI_LAT, "tourist-A", 15.0, state=fleet
    )
    did = assignment["driver_id"]
    assert await fleet.redis.exists(fleet.lock_key(did))

    # The trip ended elsewhere; the driver pings back with its new position.
    await fleet.update_driver_location(did, 78.1193, 9.9252, AVAILABLE)
    assert not await fleet.redis.exists(fleet.lock_key(did))

    # ...and is immediately claimable again (SET NX now succeeds).
    again = await dispatch_cab(78.1193, 9.9252, "tourist-B", 15.0, state=fleet)
    assert again["driver_id"] == did


async def test_book_cab_carries_drop_off_coordinates(fleet: FleetState) -> None:
    """``release_cab`` needs the destination hub to reposition the driver."""
    from app.fleet.state import set_fleet_state
    from app.workflows.activities import book_cab

    set_fleet_state(fleet)
    try:
        receipt = await book_cab("itin-book-drop", "MAS", "MDU", "waitlist_dropped")
    finally:
        set_fleet_state(None)

    assert receipt["driver_id"]
    assert receipt["drop_lon"] == pytest.approx(78.1193)  # Madurai Junction
    assert receipt["drop_lat"] == pytest.approx(9.9252)


async def test_release_cab_returns_driver_to_pool(fleet: FleetState) -> None:
    """The trip-end release: lease dropped, driver AVAILABLE, index clean."""
    from app.fleet.state import set_fleet_state
    from app.workflows.activities import book_cab, release_cab

    set_fleet_state(fleet)
    try:
        receipt = await book_cab("itin-book-rel", "MAS", "MDU", "waitlist_dropped")
        assert await fleet.locked_count() == 1
        out = await release_cab(
            "itin-book-rel",
            receipt["driver_id"],
            receipt["drop_lon"],
            receipt["drop_lat"],
        )
    finally:
        set_fleet_state(None)

    assert out["released"] is True
    driver = await fleet.get_driver(receipt["driver_id"])
    assert driver["status"] == AVAILABLE
    assert await fleet.available_count() == 5
    assert await fleet.locked_count() == 0
    assert not await fleet.redis.exists(fleet.lock_key(receipt["driver_id"]))

    # Idempotent: releasing again is a harmless no-op, not an error.
    set_fleet_state(fleet)
    try:
        again = await release_cab("itin-book-rel", receipt["driver_id"])
    finally:
        set_fleet_state(None)
    assert again["released"] is True

