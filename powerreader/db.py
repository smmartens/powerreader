from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aiosqlite

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_readings (
    id INTEGER PRIMARY KEY,
    device_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    total_in REAL,
    total_out REAL,
    power_w REAL,
    voltage REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS hourly_agg (
    id INTEGER PRIMARY KEY,
    device_id TEXT NOT NULL,
    hour TEXT NOT NULL,
    avg_power_w REAL,
    kwh_consumed REAL,
    reading_count INTEGER,
    coverage_seconds INTEGER,
    UNIQUE(device_id, hour)
);

CREATE TABLE IF NOT EXISTS daily_agg (
    id INTEGER PRIMARY KEY,
    device_id TEXT NOT NULL,
    date TEXT NOT NULL,
    avg_power_w REAL,
    kwh_consumed REAL,
    reading_count INTEGER,
    UNIQUE(device_id, date)
);

CREATE TABLE IF NOT EXISTS mqtt_log (
    id INTEGER PRIMARY KEY,
    timestamp TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    device_id TEXT,
    status TEXT NOT NULL,
    summary TEXT,
    topic TEXT
);
"""

_INSERT_READING_SQL = """INSERT INTO raw_readings
   (device_id, timestamp, total_in, total_out, power_w, voltage)
   VALUES (?, ?, ?, ?, ?, ?)"""

_INSERT_LOG_SQL = """INSERT INTO mqtt_log (device_id, status, summary, topic)
   VALUES (?, ?, ?, ?)"""


@asynccontextmanager
async def _connect(
    db_path: str, *, row_factory: bool = False
) -> AsyncIterator[aiosqlite.Connection]:
    """Open a database connection with optional Row factory."""
    async with aiosqlite.connect(db_path) as db:
        if row_factory:
            db.row_factory = aiosqlite.Row
        yield db


async def _fetch_one(cursor: aiosqlite.Cursor) -> dict | None:
    row = await cursor.fetchone()
    return dict(row) if row else None


async def _fetch_all(cursor: aiosqlite.Cursor) -> list[dict]:
    return [dict(r) for r in await cursor.fetchall()]


async def init_db(db_path: str) -> None:
    """Create tables if they don't exist."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.executescript(_SCHEMA_SQL)
        # Migration: add coverage_seconds to existing hourly_agg tables
        try:
            await db.execute(
                "ALTER TABLE hourly_agg ADD COLUMN coverage_seconds INTEGER"
            )
            await db.commit()
        except Exception:  # noqa: BLE001
            pass  # Column already exists


async def insert_reading(
    db_path: str,
    device_id: str,
    timestamp: str,
    total_in: float | None = None,
    total_out: float | None = None,
    power_w: float | None = None,
    voltage: float | None = None,
) -> int:
    """Insert a raw reading and return its row id."""
    async with _connect(db_path) as db:
        cursor = await db.execute(
            _INSERT_READING_SQL,
            (device_id, timestamp, total_in, total_out, power_w, voltage),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore[return-value]


async def insert_reading_and_log(
    db_path: str,
    device_id: str,
    timestamp: str,
    total_in: float | None = None,
    total_out: float | None = None,
    power_w: float | None = None,
    voltage: float | None = None,
    log_status: str = "ok",
    log_summary: str | None = None,
    log_topic: str | None = None,
) -> int:
    """Insert a raw reading and an MQTT log entry in a single transaction."""
    async with _connect(db_path) as db:
        await db.execute("PRAGMA busy_timeout=5000")
        cursor = await db.execute(
            _INSERT_READING_SQL,
            (device_id, timestamp, total_in, total_out, power_w, voltage),
        )
        await db.execute(
            _INSERT_LOG_SQL,
            (device_id, log_status, log_summary, log_topic),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore[return-value]


async def get_latest_reading(db_path: str, device_id: str | None = None) -> dict | None:
    """Return the most recent raw reading, optionally filtered by device."""
    async with _connect(db_path, row_factory=True) as db:
        if device_id is not None:
            cursor = await db.execute(
                """SELECT * FROM raw_readings
                   WHERE device_id = ?
                   ORDER BY timestamp DESC LIMIT 1""",
                (device_id,),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM raw_readings ORDER BY timestamp DESC LIMIT 1"
            )
        return await _fetch_one(cursor)


async def get_readings(
    db_path: str, device_id: str, start: str, end: str
) -> list[dict]:
    """Return raw readings for a device within [start, end] time range."""
    async with _connect(db_path, row_factory=True) as db:
        cursor = await db.execute(
            """SELECT * FROM raw_readings
               WHERE device_id = ? AND timestamp >= ? AND timestamp <= ?
               ORDER BY timestamp""",
            (device_id, start, end),
        )
        return await _fetch_all(cursor)


async def get_hourly_agg(
    db_path: str, device_id: str, start: str, end: str
) -> list[dict]:
    """Return hourly aggregates for a device within [start, end] time range."""
    async with _connect(db_path, row_factory=True) as db:
        cursor = await db.execute(
            """SELECT * FROM hourly_agg
               WHERE device_id = ? AND hour >= ? AND hour <= ?
               ORDER BY hour""",
            (device_id, start, end),
        )
        return await _fetch_all(cursor)


async def get_hourly_agg_by_hour_of_day(
    db_path: str, device_id: str, start: str, end: str
) -> list[dict]:
    """Aggregate hourly_agg rows by hour-of-day (0-23) within a date range."""
    async with _connect(db_path, row_factory=True) as db:
        cursor = await db.execute(
            """SELECT
                CAST(strftime('%H', hour || ':00:00') AS INTEGER) AS hour_of_day,
                ROUND(AVG(avg_power_w), 1) AS avg_power_w,
                ROUND(SUM(kwh_consumed), 3) AS total_kwh,
                SUM(reading_count) AS reading_count,
                COUNT(*) AS days_covered,
                ROUND(AVG(coverage_seconds), 0) AS avg_coverage_seconds
            FROM hourly_agg
            WHERE device_id = ? AND hour >= ? AND hour <= ?
            GROUP BY hour_of_day
            ORDER BY hour_of_day""",
            (device_id, start, end),
        )
        return await _fetch_all(cursor)


async def get_daily_agg(
    db_path: str, device_id: str, start: str, end: str
) -> list[dict]:
    """Return daily aggregates for a device within [start, end] date range."""
    async with _connect(db_path, row_factory=True) as db:
        cursor = await db.execute(
            """SELECT * FROM daily_agg
               WHERE device_id = ? AND date >= ? AND date <= ?
               ORDER BY date""",
            (device_id, start, end),
        )
        return await _fetch_all(cursor)


async def get_consumption_stats(db_path: str, device_id: str, year: int) -> dict:
    """Return consumption stats: avg kWh/day, avg kWh/month, kWh this year."""
    year_start = f"{year}-01-01"
    async with _connect(db_path, row_factory=True) as db:
        # Average kWh per day from daily_agg
        cursor = await db.execute(
            "SELECT AVG(kwh_consumed) AS avg_kwh_per_day"
            " FROM daily_agg WHERE device_id = ?",
            (device_id,),
        )
        row = await cursor.fetchone()
        avg_day = row["avg_kwh_per_day"] if row else None

        # Consumption this year: diff between first total_in on/after Jan 1
        # (or first available) and latest total_in
        cursor = await db.execute(
            "SELECT total_in FROM raw_readings"
            " WHERE device_id = ? AND timestamp >= ? AND total_in IS NOT NULL"
            " ORDER BY timestamp ASC LIMIT 1",
            (device_id, year_start),
        )
        first_row = await cursor.fetchone()
        if first_row is None:
            # Fall back to the very first reading available
            cursor = await db.execute(
                "SELECT total_in FROM raw_readings"
                " WHERE device_id = ? AND total_in IS NOT NULL"
                " ORDER BY timestamp ASC LIMIT 1",
                (device_id,),
            )
            first_row = await cursor.fetchone()

        cursor = await db.execute(
            "SELECT total_in FROM raw_readings"
            " WHERE device_id = ? AND total_in IS NOT NULL"
            " ORDER BY timestamp DESC LIMIT 1",
            (device_id,),
        )
        last_row = await cursor.fetchone()

        kwh_year = None
        if first_row and last_row:
            kwh_year = last_row["total_in"] - first_row["total_in"]

    return {
        "avg_kwh_per_day": round(avg_day, 2) if avg_day is not None else None,
        "avg_kwh_per_month": round(avg_day * 30.44, 2) if avg_day is not None else None,
        "kwh_this_year": round(kwh_year, 2) if kwh_year is not None else None,
    }


async def insert_mqtt_log(
    db_path: str,
    device_id: str | None,
    status: str,
    summary: str | None,
    topic: str | None,
) -> int:
    """Insert an MQTT log entry and return its row id."""
    async with _connect(db_path) as db:
        cursor = await db.execute(
            _INSERT_LOG_SQL,
            (device_id, status, summary, topic),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore[return-value]


async def get_days_by_consumption(
    db_path: str, device_id: str, limit: int = 5, *, ascending: bool = False
) -> list[dict]:
    """Return top N days by kWh consumed, ordered highest-first or lowest-first."""
    order = "ASC" if ascending else "DESC"
    async with _connect(db_path, row_factory=True) as db:
        cursor = await db.execute(
            "SELECT date, kwh_consumed FROM daily_agg"
            " WHERE device_id = ? AND kwh_consumed IS NOT NULL"
            " ORDER BY kwh_consumed " + order + " LIMIT ?",
            (device_id, limit),
        )
        return await _fetch_all(cursor)


async def get_coverage_stats(db_path: str, device_id: str) -> dict:
    """Return coverage stats: first date in hourly_agg and count of fully-covered days.

    A day is 'fully covered' when all 24 hourly buckets exist and each has
    reading_count >= 3, indicating complete and reliable data for that day.
    """
    async with _connect(db_path, row_factory=True) as db:
        cursor = await db.execute(
            "SELECT substr(MIN(hour), 1, 10) AS first_reading_date"
            " FROM hourly_agg WHERE device_id = ?",
            (device_id,),
        )
        row = await cursor.fetchone()
        val = row["first_reading_date"] if row else None
        first_date = val if val else None

        cursor = await db.execute(
            """
            SELECT COUNT(*) AS days_with_full_coverage
            FROM (
                SELECT substr(hour, 1, 10) AS date
                FROM hourly_agg
                WHERE device_id = ?
                GROUP BY date
                HAVING COUNT(*) = 24 AND MIN(reading_count) >= 3
            )
            """,
            (device_id,),
        )
        row = await cursor.fetchone()
        full_coverage = row["days_with_full_coverage"] if row else 0

    return {"first_reading_date": first_date, "days_with_full_coverage": full_coverage}


async def get_earliest_date(db_path: str, device_id: str) -> str | None:
    """Return the earliest date in hourly_agg for a device (YYYY-MM-DD), or None."""
    async with _connect(db_path, row_factory=True) as db:
        cursor = await db.execute(
            "SELECT substr(MIN(hour), 1, 10) AS earliest_date"
            " FROM hourly_agg WHERE device_id = ?",
            (device_id,),
        )
        row = await cursor.fetchone()
        return row["earliest_date"] if row and row["earliest_date"] else None


async def get_mqtt_log(db_path: str, limit: int = 200) -> list[dict]:
    """Return recent MQTT log entries ordered by id descending."""
    async with _connect(db_path, row_factory=True) as db:
        cursor = await db.execute(
            "SELECT * FROM mqtt_log ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return await _fetch_all(cursor)
