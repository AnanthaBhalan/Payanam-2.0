"""Fleet/driver HTTP surface.

``POST /api/v1/driver/location`` ingests live GPS pings into the Redis
geospatial index. Drivers post their own position; dispatch then reads the
index rather than polling anyone.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from ..fleet.state import AVAILABLE, FleetState, get_fleet_state

log = logging.getLogger("payanam.api.fleet")
router = APIRouter(prefix="/api/v1/driver", tags=["fleet"])


class DriverLocation(BaseModel):
    """A single GPS ping from a driver."""

    driver_id: str = Field(min_length=1, max_length=64)
    lon: float = Field(ge=-180.0, le=180.0)
    lat: float = Field(ge=-90.0, le=90.0)
    status: str = Field(default=AVAILABLE)
    # Optional live occupancy signal for future supply/demand balancing.
    vehicle_model: Optional[str] = None


class DriverLocationBatch(BaseModel):
    drivers: List[DriverLocation] = Field(min_length=1, max_length=500)


class DispatchPreview(BaseModel):
    """A dry-run candidate lookup; performs no locking."""

    lon: float = Field(ge=-180.0, le=180.0)
    lat: float = Field(ge=-90.0, le=90.0)
    radius_km: float = Field(default=15.0, gt=0.0, le=200.0)
    limit: int = Field(default=10, ge=1, le=100)

@router.post(
    "/location",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a driver GPS ping into the fleet index",
)
async def upsert_driver_location(ping: DriverLocation) -> Dict[str, Any]:
    """Record one driver position and (re)index them for dispatch."""
    state = get_fleet_state()
    try:
        result = await state.update_driver_location(
            ping.driver_id, ping.lon, ping.lat, ping.status
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to record driver location")
        raise HTTPException(
            status_code=503, detail=f"fleet index unavailable: {exc}"
        ) from exc

    if ping.vehicle_model:
        try:
            await state.redis.hset(
                state.driver_key(ping.driver_id), "vehicle_model", ping.vehicle_model
            )
        except Exception:  # noqa: BLE001 - metadata is best-effort
            log.debug("vehicle_model not persisted for %s", ping.driver_id)

    return {"accepted": True, **result}


@router.post(
    "/locations",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Bulk driver GPS ingest",
)
async def upsert_driver_locations(batch: DriverLocationBatch) -> Dict[str, Any]:
    """Record many pings in one call (fleet phones syncing in bulk)."""
    state = get_fleet_state()
    recorded: List[Dict[str, Any]] = []
    for ping in batch.drivers:
        recorded.append(
            await state.update_driver_location(
                ping.driver_id, ping.lon, ping.lat, ping.status
            )
        )
    return {"accepted": len(recorded), "drivers": recorded}


@router.get(
    "/nearby",
    summary="Preview dispatchable drivers near a point (no locking)",
)
async def nearby_drivers(
    lon: float = Query(ge=-180.0, le=180.0),
    lat: float = Query(ge=-90.0, le=90.0),
    radius_km: float = Query(default=15.0, gt=0.0, le=200.0),
    limit: int = Query(default=10, ge=1, le=100),
) -> Dict[str, Any]:
    """Dry run of the spatial query -- does not claim any driver."""
    state = get_fleet_state()
    candidates = await state.find_nearest_available(
        lon, lat, radius_km=radius_km, limit=limit
    )
    return {
        "center": {"lon": lon, "lat": lat},
        "radius_km": radius_km,
        "count": len(candidates),
        "drivers": candidates,
    }


@router.get("/fleet/stats", summary="Fleet index occupancy")
async def fleet_stats() -> Dict[str, Any]:
    state = get_fleet_state()
    return {
        "available": await state.available_count(),
        "locked": await state.locked_count(),
    }
