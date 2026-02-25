import csv
import io
from collections.abc import AsyncIterator
from datetime import date, datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Request
from starlette.responses import StreamingResponse

from powerreader import __version__, db

router = APIRouter(prefix="/api")

_RANGE_MAP = {
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}


_MAX_DEVICE_ID_LEN = 64


def _clamp(value: int, lo: int, hi: int) -> int:
    """Clamp *value* to [lo, hi]."""
    return max(lo, min(value, hi))


async def _resolve_device_id(db_path: str, device_id: str | None) -> str | None:
    """Return the given device_id, or auto-detect from the latest reading."""
    if device_id is not None:
        return device_id[:_MAX_DEVICE_ID_LEN]
    latest = await db.get_latest_reading(db_path, None)
    return latest["device_id"] if latest else None


@router.get("/version")
async def version_info() -> dict:
    return {"version": __version__}


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
        for row in data:
            row["bucket"] = row["date"]
    else:
        data = await db.get_hourly_agg(
            request.app.state.db_path,
            resolved,
            start.strftime("%Y-%m-%dT%H"),
            now.strftime("%Y-%m-%dT%H"),
        )
        for row in data:
            row["bucket"] = row["hour"]

    return {"range": range, "data": data}


@router.get("/averages")
async def averages(
    request: Request,
    device_id: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict:
    from_date_parsed = _parse_date(from_date) if from_date is not None else None
    to_date_parsed = _parse_date(to_date) if to_date is not None else None

    resolved = await _resolve_device_id(request.app.state.db_path, device_id)
    if resolved is None:
        today = date.today().isoformat()
        return {
            "device_id": device_id,
            "from_date": from_date_parsed.isoformat() if from_date_parsed else today,
            "to_date": to_date_parsed.isoformat() if to_date_parsed else today,
            "data": [],
        }

    today = date.today()
    if from_date_parsed is None:
        earliest = await db.get_earliest_date(request.app.state.db_path, resolved)
        from_date_parsed = date.fromisoformat(earliest) if earliest else today
    if to_date_parsed is None:
        to_date_parsed = today

    if from_date_parsed > to_date_parsed:
        raise HTTPException(
            status_code=400, detail="from_date must not be after to_date"
        )

    data = await db.get_hourly_agg_by_hour_of_day(
        request.app.state.db_path,
        resolved,
        from_date_parsed.isoformat() + "T00",
        to_date_parsed.isoformat() + "T23",
    )
    return {
        "device_id": resolved,
        "from_date": from_date_parsed.isoformat(),
        "to_date": to_date_parsed.isoformat(),
        "data": data,
    }


@router.get("/stats")
async def consumption_stats(request: Request, device_id: str | None = None) -> dict:
    resolved = await _resolve_device_id(request.app.state.db_path, device_id)
    if resolved is None:
        return {
            "device_id": device_id,
            "avg_kwh_per_day": None,
            "avg_kwh_per_month": None,
            "kwh_this_year": None,
            "first_reading_date": None,
            "days_since_first_reading": None,
            "days_with_full_coverage": None,
        }
    year = datetime.now().year
    stats = await db.get_consumption_stats(request.app.state.db_path, resolved, year)
    coverage = await db.get_coverage_stats(request.app.state.db_path, resolved)
    days_since = None
    if coverage["first_reading_date"]:
        first = date.fromisoformat(coverage["first_reading_date"])
        days_since = (date.today() - first).days
    return {
        "device_id": resolved,
        **stats,
        **coverage,
        "days_since_first_reading": days_since,
    }


@router.get("/records")
async def consumption_records(request: Request, device_id: str | None = None) -> dict:
    resolved = await _resolve_device_id(request.app.state.db_path, device_id)
    if resolved is None:
        return {"device_id": device_id, "highest": [], "lowest": []}
    highest = await db.get_days_by_consumption(
        request.app.state.db_path, resolved, ascending=False
    )
    lowest = await db.get_days_by_consumption(
        request.app.state.db_path, resolved, ascending=True
    )
    return {"device_id": resolved, "highest": highest, "lowest": lowest}


@router.get("/log")
async def mqtt_log(request: Request, limit: int = 200) -> dict:
    limit = _clamp(limit, 1, 1000)
    data = await db.get_mqtt_log(request.app.state.db_path, limit=limit)
    return {"data": data}


# --- CSV Export ---

_CSV_COLUMNS_HOURLY = [
    "hour_of_day",
    "avg_power_w",
    "total_kwh",
    "reading_count",
    "days_covered",
    "avg_coverage_seconds",
]


_MAX_EXPORT_DAYS = 3650


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD string into a date, or raise 400."""
    try:
        return date.fromisoformat(value[:10])
    except (ValueError, TypeError) as err:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: '{value[:10]}'. Use YYYY-MM-DD.",
        ) from err


async def _generate_hourly_csv(
    db_path: str, device_id: str, start: date, end: date
) -> AsyncIterator[str]:
    """Yield CSV rows aggregated by hour-of-day (0-23)."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_CSV_COLUMNS_HOURLY)
    yield buf.getvalue()
    buf.seek(0)
    buf.truncate()

    start_hour = start.isoformat() + "T00"
    end_hour = end.isoformat() + "T23"
    rows = await db.get_hourly_agg_by_hour_of_day(
        db_path, device_id, start_hour, end_hour
    )
    for row in rows:
        writer.writerow([row.get(col) for col in _CSV_COLUMNS_HOURLY])
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate()


_REPORT_GENERATORS = {
    "hourly": _generate_hourly_csv,
}


@router.get("/export")
async def export_csv(
    request: Request,
    start: str = Query(...),
    end: str = Query(...),
    report: str = Query("hourly"),
    device_id: str | None = None,
) -> StreamingResponse:
    if report not in _REPORT_GENERATORS:
        valid = ", ".join(sorted(_REPORT_GENERATORS))
        raise HTTPException(
            status_code=400,
            detail=f"Invalid report type '{report}'. Must be one of: {valid}",
        )

    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if start_date > end_date:
        raise HTTPException(
            status_code=400, detail="start date must not be after end date"
        )
    if (end_date - start_date).days > _MAX_EXPORT_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Date range must not exceed {_MAX_EXPORT_DAYS} days",
        )

    resolved = await _resolve_device_id(request.app.state.db_path, device_id)
    if resolved is None:
        raise HTTPException(status_code=404, detail="No device found")

    generator = _REPORT_GENERATORS[report]
    filename = f"powerreader_{report}_{start_date}_{end_date}.csv"
    return StreamingResponse(
        generator(request.app.state.db_path, resolved, start_date, end_date),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
