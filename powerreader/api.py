from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Request

from powerreader import db
from powerreader.aggregation import get_avg_by_time_of_day

router = APIRouter(prefix="/api")

_RANGE_MAP = {
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}


async def _resolve_device_id(db_path: str, device_id: str | None) -> str | None:
    """Return the given device_id, or auto-detect from the latest reading."""
    if device_id is not None:
        return device_id
    latest = await db.get_latest_reading(db_path, None)
    return latest["device_id"] if latest else None


@router.get("/current")
async def current_reading(request: Request, device_id: str | None = None) -> dict:
    reading = await db.get_latest_reading(request.app.state.db_path, device_id)
    if reading is None:
        raise HTTPException(status_code=404, detail="No readings found")
    return reading


@router.get("/history")
async def history(
    request: Request, device_id: str | None = None, range: str = "24h"
) -> dict:
    if range not in _RANGE_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid range '{range}'. Must be one of: {', '.join(_RANGE_MAP)}",
        )

    resolved = await _resolve_device_id(request.app.state.db_path, device_id)
    if resolved is None:
        return {"range": range, "data": []}

    now = datetime.now()
    start = now - _RANGE_MAP[range]

    if range == "30d":
        data = await db.get_daily_agg(
            request.app.state.db_path,
            resolved,
            start.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        )
    else:
        data = await db.get_hourly_agg(
            request.app.state.db_path,
            resolved,
            start.strftime("%Y-%m-%dT%H"),
            now.strftime("%Y-%m-%dT%H"),
        )

    return {"range": range, "data": data}


@router.get("/averages")
async def averages(
    request: Request, device_id: str | None = None, days: int = 30
) -> dict:
    resolved = await _resolve_device_id(request.app.state.db_path, device_id)
    if resolved is None:
        return {"device_id": device_id, "days": days, "data": []}
    data = await get_avg_by_time_of_day(request.app.state.db_path, resolved, days)
    return {"device_id": resolved, "days": days, "data": data}


@router.get("/log")
async def mqtt_log(request: Request, limit: int = 200) -> dict:
    data = await db.get_mqtt_log(request.app.state.db_path, limit=limit)
    return {"data": data}
