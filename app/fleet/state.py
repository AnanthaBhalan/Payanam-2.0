"""Fleet state: Redis geospatial index of live drivers.

Key layout
----------
``payanam:fleet:available``   GEO set  -- drivers currently dispatchable
``payanam:fleet:locked``      GEO set  -- drivers assigned to a tourist
``payanam:fleet:driver:{id}`` HASH    -- driver metadata (lon/lat/status/...)
``payanam:fleet:lock:{id}``    STRING  -- the dispatch lease for a held driver

A driver is moved between the two GEO sets in the same breath as its status
change, so ``GEOSEARCH`` on ``available`` can never surface a driver another
workflow is mid-claim on.

The client factory prefers a real Redis and falls back to ``fakeredis`` so the
stack and the test suite run with no Redis server present.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ..config import Settings, get_settings

log = logging.getLogger("payanam.fleet")

AVAILABLE = "AVAILABLE"
ASSIGNED = "ASSIGNED"
OFFLINE = "OFFLINE"

# Statuses a driver can be dispatched from. Anything else is not dispatchable.
_DISPATCHABLE = {AVAILABLE, "IDLE", "ONLINE"}


def _decode(value: Any) -> Any:
    """Redis returns bytes; normalise to str so callers see plain data."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


class FleetState:
    """Geospatial driver index backed by Redis."""

    def __init__(self, redis: Any, settings: Optional[Settings] = None) -> None:
        self.redis = redis
        self.settings = settings or get_settings()

    # ------------------------------------------------------------------ keys
    @property
    def geo_available(self) -> str:
        return self.settings.fleet_geo_key

    @property
    def geo_locked(self) -> str:
        return self.settings.fleet_locked_key

    def driver_key(self, driver_id: str) -> str:
        return f"{self.settings.fleet_driver_prefix}{driver_id}"

    def lock_key(self, driver_id: str) -> str:
        return f"{self.settings.fleet_lock_prefix}{driver_id}"

    # ----------------------------------------------------------------- writes
    async def update_driver_location(
        self,
        driver_id: str,
        lon: float,
        lat: float,
        status: str = AVAILABLE,
    ) -> Dict[str, Any]:
        """Record a GPS ping and (re)publish the driver in the right index.

        Idempotent per driver: re-pinging moves the member between the
        available and locked sets to match ``status``.
        """
        status = (status or AVAILABLE).upper()
        await self.redis.hset(
            self.driver_key(driver_id),
            mapping={
                "driver_id": driver_id,
                "lon": str(float(lon)),
                "lat": str(float(lat)),
                "status": status,
            },
        )
        dispatchable = status in _DISPATCHABLE
        target = self.geo_available if dispatchable else self.geo_locked
        other = self.geo_locked if dispatchable else self.geo_available

        # Add to the target before removing from the other so a concurrent
        # GEOSEARCH sees the driver in exactly one set at all times.
        await self.redis.geoadd(target, (float(lon), float(lat), driver_id))
        await self.redis.zrem(other, driver_id)
        return {
            "driver_id": driver_id,
            "lon": float(lon),
            "lat": float(lat),
            "status": status,
            "dispatchable": dispatchable,
        }

    async def get_driver(self, driver_id: str) -> Optional[Dict[str, Any]]:
        raw = await self.redis.hgetall(self.driver_key(driver_id))
        if not raw:
            return None
        out: Dict[str, Any] = {_decode(k): _decode(v) for k, v in raw.items()}
        for field in ("lon", "lat"):
            if field in out:
                try:
                    out[field] = float(out[field])
                except (TypeError, ValueError):
                    out[field] = 0.0
        return out

    async def set_status(self, driver_id: str, status: str) -> None:
        """Status-only change (no GPS ping); keeps the GEO sets consistent."""
        driver = await self.get_driver(driver_id)
        if driver is None:
            raise KeyError(f"unknown driver {driver_id}")
        status = status.upper()
        await self.redis.hset(self.driver_key(driver_id), "status", status)
        dispatchable = status in _DISPATCHABLE
        target = self.geo_available if dispatchable else self.geo_locked
        other = self.geo_locked if dispatchable else self.geo_available
        await self.redis.geoadd(target, (driver["lon"], driver["lat"], driver_id))
        await self.redis.zrem(other, driver_id)

    async def release_driver(self, driver_id: str) -> bool:
        """Return a driver to the available pool (drops any lease)."""
        await self.redis.delete(self.lock_key(driver_id))
        driver = await self.get_driver(driver_id)
        if driver is None:
            return False
        await self.set_status(driver_id, AVAILABLE)
        return True

    # ------------------------------------------------------------------ reads
    async def find_nearest_available(
        self,
        lon: float,
        lat: float,
        radius_km: float = 15.0,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """``GEOSEARCH`` the available set, nearest first.

        Candidates carry ``distance_km``, ordered ascending. Drivers already
        locked by another workflow are not in this index, so they never
        appear here.
        """
        if radius_km <= 0 or limit <= 0:
            return []
        raw = await self.redis.geosearch(
            self.geo_available,
            longitude=float(lon),
            latitude=float(lat),
            radius=float(radius_km),
            unit="km",
            withdist=True,
            sort="ASC",
            count=int(limit),
        )
        candidates: List[Dict[str, Any]] = []
        for entry in raw or []:
            # withdist yields [member, distance] pairs
            if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                member, distance = entry[0], entry[1]
            else:  # pragma: no cover - server not returning pairs
                member, distance = entry, 0.0
            driver_id = _decode(member)
            driver = await self.get_driver(driver_id) or {"driver_id": driver_id}
            driver["distance_km"] = round(float(distance), 4)
            candidates.append(driver)
        return candidates

    async def available_count(self) -> int:
        return int(await self.redis.zcard(self.geo_available) or 0)

    async def locked_count(self) -> int:
        return int(await self.redis.zcard(self.geo_locked) or 0)

    async def clear(self) -> None:
        """Drop the whole index (test hygiene)."""
        await self.redis.delete(self.geo_available, self.geo_locked)
        for pattern in (
            f"{self.settings.fleet_driver_prefix}*",
            f"{self.settings.fleet_lock_prefix}*",
        ):
            for key in await self.redis.keys(pattern):
                await self.redis.delete(key)


# --------------------------------------------------------------------------- #
# Client factory
# --------------------------------------------------------------------------- #
_STATE: Optional[FleetState] = None


def get_fleet_state(
    settings: Optional[Settings] = None,
    client: Optional[Any] = None,
    refresh: bool = False,
) -> FleetState:
    """Return the active fleet state, building the client on first use."""
    global _STATE
    if _STATE is not None and not refresh and client is None:
        return _STATE
    settings = settings or get_settings()
    _STATE = FleetState(client or _build_client(settings), settings)
    return _STATE


def set_fleet_state(state: Optional[FleetState]) -> None:
    """Override the active fleet state (tests, and app startup wiring)."""
    global _STATE
    _STATE = state


def _build_client(settings: Settings) -> Any:
    """Async Redis client, falling back to in-process fakeredis."""
    try:
        import redis.asyncio as aioredis

        log.info("fleet index using redis at %s", settings.redis_url)
        return aioredis.from_url(settings.redis_url, decode_responses=False)
    except Exception as exc:  # noqa: BLE001 - degrade, never block startup
        log.warning("redis unavailable (%s); using in-process fakeredis", exc)
        import fakeredis.aioredis as fakeaioredis

        return fakeaioredis.FakeRedis()

