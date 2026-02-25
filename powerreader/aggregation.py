from apscheduler.schedulers.asyncio import AsyncIOScheduler

from powerreader.db import _connect


async def compute_hourly_agg(db_path: str) -> int:
    """Compute hourly aggregates from raw_readings. Returns rows upserted."""
    async with _connect(db_path) as db:
        cursor = await db.execute(
            """
            INSERT OR REPLACE INTO hourly_agg
                (device_id, hour, avg_power_w,
                 kwh_consumed, reading_count, coverage_seconds)
            SELECT
                device_id,
                strftime('%Y-%m-%dT%H', timestamp) AS hour,
                (MAX(total_in) - MIN(total_in)) * 1000,  -- kWh delta → Wh (avg_power_w)
                MAX(total_in) - MIN(total_in),            -- kWh consumed this hour
                COUNT(*),
                -- Seconds between first and last reading in the hour
                CAST(
                    strftime('%s', MAX(timestamp))
                    - strftime('%s', MIN(timestamp))
                AS INTEGER)
            FROM raw_readings
            WHERE total_in IS NOT NULL
            GROUP BY device_id, strftime('%Y-%m-%dT%H', timestamp)
            """
        )
        await db.commit()
        return cursor.rowcount


async def compute_daily_agg(db_path: str) -> int:
    """Compute daily aggregates from hourly_agg. Returns rows upserted."""
    async with _connect(db_path) as db:
        cursor = await db.execute(
            """
            INSERT OR REPLACE INTO daily_agg
                (device_id, date, avg_power_w,
                 kwh_consumed, reading_count)
            SELECT
                device_id,
                substr(hour, 1, 10) AS date,
                AVG(avg_power_w),
                SUM(kwh_consumed),
                SUM(reading_count)
            FROM hourly_agg
            GROUP BY device_id, substr(hour, 1, 10)
            """
        )
        await db.commit()
        return cursor.rowcount


async def prune_raw_readings(db_path: str, retention_days: int) -> int:
    """Delete raw readings older than retention_days. Returns rows deleted."""
    async with _connect(db_path) as db:
        cursor = await db.execute(
            "DELETE FROM raw_readings"
            " WHERE timestamp < datetime('now', 'localtime', ?)",
            (f"-{retention_days} days",),
        )
        await db.commit()
        return cursor.rowcount


async def prune_mqtt_log(db_path: str, retention_days: int) -> int:
    """Delete mqtt_log entries older than retention_days. Returns rows deleted."""
    async with _connect(db_path) as db:
        cursor = await db.execute(
            "DELETE FROM mqtt_log WHERE timestamp < datetime('now', 'localtime', ?)",
            (f"-{retention_days} days",),
        )
        await db.commit()
        return cursor.rowcount


def setup_scheduler(db_path: str, retention_days: int) -> AsyncIOScheduler:
    """Create and configure the aggregation scheduler (caller starts it)."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        compute_hourly_agg,
        "interval",
        minutes=10,
        args=[db_path],
        id="hourly_agg",
    )
    scheduler.add_job(
        compute_daily_agg,
        "interval",
        minutes=60,
        args=[db_path],
        id="daily_agg",
    )
    scheduler.add_job(
        prune_raw_readings,
        "interval",
        hours=24,
        args=[db_path, retention_days],
        id="prune_raw",
    )
    scheduler.add_job(
        prune_mqtt_log,
        "interval",
        hours=24,
        args=[db_path, retention_days],
        id="prune_mqtt_log",
    )
    return scheduler
