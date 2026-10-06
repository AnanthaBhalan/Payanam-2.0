"""Fleet matching: assign a tourist to the nearest *free* cab, atomically.

Why this is not just "GEOSEARCH and take the first hit"
-------------------------------------------------------
Two Temporal workflows can -- and routinely do -- reach the fallback cab leg at
the same instant. A plain ``GEOSEARCH`` followed by a ``SET`` is a classic
TOCTOU race: both see driver D1 as nearest, both write, and the tourist ends up
with two cabs dispatched for one seat.

The claim below is therefore an optimistic transaction:

1. ``WATCH`` the driver's hash key.
2. ``MULTI`` -- queue the status flip, the GEO-set move and the lease write.
3. ``EXEC`` -- Redis applies it only if nobody touched the watched key.
4. On ``WatchError`` someone else won; move to the next candidate.

The lease (``SET NX EX``) is a second, independent guard: it bounds how long a
crashed workflow can strand a driver, so an abandoned hold self-heals via TTL
rather than needing a janitor.

Bipartite matching
------------------
:func:`match_batch` exposes the general form -- many tourists against many
drivers -- and resolves it greedily over globally sorted (distance) edges. For
the single-tourist case this is optimal, because the nearest free driver is by
definition the cheapest available edge.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Dict, List, Optional, Sequence

from redis.exceptions import WatchError

from .state import ASSIGNED, AVAILABLE, FleetState, get_fleet_state

log = logging.getLogger("payanam.fleet.matcher")

DEFAULT_RADIUS_KM = 15.0
DEFAULT_CANDIDATES = 10
MAX_CANDIDATE_ROUNDS = 4


class NoFleetAvailableError(Exception):
    """No dispatchable cab within the search radius.

    Raised by :func:`dispatch_cab`; ``book_cab`` converts it into the
    terminal ``NO_FLEET_AVAILABLE`` application error the workflow expects.
    """

    code = "NO_FLEET_AVAILABLE"

    def __init__(self, radius_km: float, lon: float, lat: float) -> None:
        self.radius_km = radius_km
        self.lon = lon
        self.lat = lat
        super().__init__(
            f"{self.code}: no cab within {radius_km:g}km of "
            f"({lon:.4f}, {lat:.4f})"
        )


async def claim_driver(
    state: FleetState,
    driver: Dict[str, Any],
    tourist_id: str,
    dispatch_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Atomically claim ``driver`` for ``tourist_id``.

    Returns the committed assignment, or ``None`` if another workflow won the
    race (the caller should then try the next-nearest candidate).
    """
    def _dec(v: Any) -> str:
        return v.decode("utf-8") if isinstance(v, bytes) else str(v)

    driver_id = driver["driver_id"]
    dispatch_id = dispatch_id or uuid.uuid4().hex[:16]
    ttl = state.settings.fleet_lock_ttl_seconds
    key = state.driver_key(driver_id)

    async with state.redis.pipeline(transaction=True) as pipe:
        try:
            await pipe.watch(key)
            raw = await pipe.hgetall(key)
            # redis-py returns bytes keys/values unless decode_responses=True.
            current = {_dec(k): _dec(v) for k, v in (raw or {}).items()}
            if not current:
                await pipe.unwatch()
                return None

            if current.get("status") not in (AVAILABLE, "IDLE", "ONLINE"):
                # Someone already took it (or it went offline).
                await pipe.unwatch()
                log.info(
                    "claim lost: %s is %s", driver_id, current.get("status")
                )
                return None

            pipe.multi()
            pipe.hset(
                key,
                mapping={
                    "status": ASSIGNED,
                    "assigned_to": tourist_id,
                    "dispatch_id": dispatch_id,
                },
            )
            # Move the member between the spatial indexes so it stops being
            # returned to other tourists' GEOSEARCH calls.
            pipe.geoadd(state.geo_locked, (driver["lon"], driver["lat"], driver_id))
            pipe.zrem(state.geo_available, driver_id)
            pipe.set(
                state.lock_key(driver_id),
                dispatch_id,
                ex=ttl,
                nx=True,
            )
            results = await pipe.execute()
        except WatchError:
            log.info("claim lost (watch): %s changed under us", driver_id)
            return None
        except asyncio.CancelledError:
            await pipe.reset()
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("claim failed for %s: %s", driver_id, exc)
            try:
                await pipe.reset()
            except Exception:  # noqa: BLE001
                pass
            return None

    # ``results`` is positional in queue order: 0 hset, 1 geoadd, 2 zrem,
    # 3 set(nx). Only the SET NX reply decides the outcome, so read it from
    # the END -- an unrelated count (e.g. zrem returning 0 when the member was
    # already absent) must never be mistaken for a failed claim.
    if not results or not results[-1]:
        log.info("claim lost (lease): %s already has a live lease", driver_id)
        return None

    assignment = dict(driver)
    assignment.update(
        {
            "status": ASSIGNED,
            "assigned_to": tourist_id,
            "dispatch_id": dispatch_id,
            "lease_ttl_s": ttl,
        }
    )
    return assignment


async def dispatch_cab(
    pickup_lon: float,
    pickup_lat: float,
    tourist_id: str,
    radius_km: float = DEFAULT_RADIUS_KM,
    limit: int = DEFAULT_CANDIDATES,
    state: Optional[FleetState] = None,
) -> Dict[str, Any]:
    """Assign the nearest *free* cab to ``tourist_id`` at the pickup point.

    Walks candidates nearest-first and atomically claims the first one it wins.
    A candidate lost to a concurrent workflow is skipped, so two simultaneous
    dispatches at the same location settle on the two closest drivers rather
    than colliding.

    Raises :class:`NoFleetAvailableError` when nothing within ``radius_km`` is
    both free and claimable.
    """
    state = state or get_fleet_state()
    tried: set = set()

    for attempt in range(MAX_CANDIDATE_ROUNDS):
        candidates = await state.find_nearest_available(
            pickup_lon, pickup_lat, radius_km=radius_km, limit=limit
        )
        # Keep the spatial ordering but drop anyone already attempted.
        fresh = [c for c in candidates if c["driver_id"] not in tried]
        if not fresh:
            break

        for driver in fresh:
            tried.add(driver["driver_id"])
            assignment = await claim_driver(state, driver, tourist_id)
            if assignment is not None:
                log.info(
                    "dispatched %s -> tourist %s (%.3fkm)",
                    assignment["driver_id"], tourist_id, assignment.get("distance_km", 0.0),
                )
                return assignment

        # Everything in range was taken between our search and our claim;
        # re-query so a newly freed driver can be picked up next round.
        log.info("dispatch retry %s for %s", attempt + 1, tourist_id)
        await asyncio.sleep(0)

    raise NoFleetAvailableError(radius_km, pickup_lon, pickup_lat)


async def release_cab(
    driver_id: str,
    state: Optional[FleetState] = None,
    drop_lon: Optional[float] = None,
    drop_lat: Optional[float] = None,
) -> bool:
    """End a dispatched trip: drop the lease and return the cab to the pool.

    The lease key (``SET NX EX``) self-heals abandoned holds when its TTL
    fires, and ``reclaim_expired_locks`` sweeps the index for leases that died
    without an explicit release -- but an explicit release is what keeps the
    fleet capacity exact run to run. Returns False only for an unknown driver.
    """
    state = state or get_fleet_state()
    ok = await state.release_driver_to(driver_id, drop_lon, drop_lat)
    if ok:
        log.info("released driver %s", driver_id)
    else:
        log.warning("release missed: unknown driver %s", driver_id)
    return ok


async def match_batch(
    requests: Sequence[Dict[str, Any]],
    radius_km: float = DEFAULT_RADIUS_KM,
    limit: int = DEFAULT_CANDIDATES,
    state: Optional[FleetState] = None,
) -> Dict[str, Any]:
    """Match several tourists against the fleet (bipartite assignment).

    Greedy over globally sorted edges: for every tourist, collect the in-radius
    candidates, sort all (distance, tourist, driver) edges ascending, then walk
    them assigning each edge whose two endpoints are both still free. A global
    order is what makes the result deterministic and closest-first, which is
    the same rule a Hungarian auction would converge on for this cost.

    Returns ``{"assignments": {tourist_id: driver}, "unmatched": [...]}``.
    """
    state = state or get_fleet_state()

    edges: List[Dict[str, Any]] = []
    for req in requests:
        found = await state.find_nearest_available(
            req["lon"], req["lat"], radius_km=radius_km, limit=limit
        )
        for driver in found:
            edges.append(
                {
                    "tourist_id": req["tourist_id"],
                    "driver": driver,
                    "distance_km": driver.get("distance_km", 0.0),
                }
            )
    edges.sort(key=lambda e: (e["distance_km"], e["tourist_id"]))

    taken_tourists: set = set()
    taken_drivers: set = set()
    assignments: Dict[str, Any] = {}
    for edge in edges:
        tourist_id = edge["tourist_id"]
        driver = edge["driver"]
        driver_id = driver["driver_id"]
        if tourist_id in taken_tourists or driver_id in taken_drivers:
            continue
        assignment = await claim_driver(state, driver, tourist_id)
        if assignment is None:
            continue
        assignments[tourist_id] = assignment
        taken_tourists.add(tourist_id)
        taken_drivers.add(driver_id)

    unmatched = [r["tourist_id"] for r in requests if r["tourist_id"] not in assignments]
    return {"assignments": assignments, "unmatched": unmatched}

