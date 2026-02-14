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
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS hourly_agg (
    id INTEGER PRIMARY KEY,
    device_id TEXT NOT NULL,
    hour TEXT NOT NULL,
    avg_power_w REAL,
    max_power_w REAL,
    min_power_w REAL,
    kwh_consumed REAL,
    reading_count INTEGER,
    UNIQUE(device_id, hour)
);

CREATE TABLE IF NOT EXISTS daily_agg (
    id INTEGER PRIMARY KEY,
    device_id TEXT NOT NULL,
    date TEXT NOT NULL,
    avg_power_w REAL,
    max_power_w REAL,
    min_power_w REAL,
    kwh_consumed REAL,
    reading_count INTEGER,
    UNIQUE(device_id, date)
);
"""


async def init_db(db_path: str) -> None:
    """Create tables if they don't exist."""
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(_SCHEMA_SQL)


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
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            """INSERT INTO raw_readings
               (device_id, timestamp, total_in, total_out, power_w, voltage)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (device_id, timestamp, total_in, total_out, power_w, voltage),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore[return-value]


async def get_latest_reading(db_path: str, device_id: str | None = None) -> dict | None:
    """Return the most recent raw reading, optionally filtered by device."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_readings(
    db_path: str, device_id: str, start: str, end: str
) -> list[dict]:
    """Return raw readings for a device within [start, end] time range."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT * FROM raw_readings
               WHERE device_id = ? AND timestamp >= ? AND timestamp <= ?
               ORDER BY timestamp""",
            (device_id, start, end),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_hourly_agg(
    db_path: str, device_id: str, start: str, end: str
) -> list[dict]:
    """Return hourly aggregates for a device within [start, end] time range."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT * FROM hourly_agg
               WHERE device_id = ? AND hour >= ? AND hour <= ?
               ORDER BY hour""",
            (device_id, start, end),
        )
        return [dict(r) for r in await cursor.fetchall()]


async def get_daily_agg(
    db_path: str, device_id: str, start: str, end: str
) -> list[dict]:
    """Return daily aggregates for a device within [start, end] date range."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT * FROM daily_agg
               WHERE device_id = ? AND date >= ? AND date <= ?
               ORDER BY date""",
            (device_id, start, end),
        )
        return [dict(r) for r in await cursor.fetchall()]
