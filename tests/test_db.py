import aiosqlite
import pytest

from powerreader.db import (
    get_latest_reading,
    get_mqtt_log,
    get_readings,
    init_db,
    insert_mqtt_log,
    insert_reading,
)


@pytest.mark.asyncio
async def test_init_db_creates_tables(initialized_db: str) -> None:
    async with aiosqlite.connect(initialized_db) as db:
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in await cursor.fetchall()]
    assert "raw_readings" in tables
    assert "hourly_agg" in tables
    assert "daily_agg" in tables


@pytest.mark.asyncio
async def test_init_db_idempotent(db_path: str) -> None:
    await init_db(db_path)
    await init_db(db_path)  # should not raise


@pytest.mark.asyncio
async def test_insert_and_get_latest(initialized_db: str) -> None:
    row_id = await insert_reading(
        initialized_db,
        device_id="meter1",
        timestamp="2024-01-15T14:30:00",
        total_in=42000.5,
        total_out=0.0,
        power_w=538.0,
        voltage=230.1,
    )
    assert row_id is not None

    latest = await get_latest_reading(initialized_db)
    assert latest is not None
    assert latest["device_id"] == "meter1"
    assert latest["power_w"] == 538.0


@pytest.mark.asyncio
async def test_get_latest_reading_by_device(initialized_db: str) -> None:
    await insert_reading(initialized_db, "meter1", "2024-01-15T14:00:00", power_w=100.0)
    await insert_reading(initialized_db, "meter2", "2024-01-15T15:00:00", power_w=200.0)

    latest = await get_latest_reading(initialized_db, device_id="meter1")
    assert latest is not None
    assert latest["device_id"] == "meter1"
    assert latest["power_w"] == 100.0


@pytest.mark.asyncio
async def test_get_latest_reading_empty_db(initialized_db: str) -> None:
    result = await get_latest_reading(initialized_db)
    assert result is None


@pytest.mark.asyncio
async def test_get_readings_time_range(initialized_db: str) -> None:
    await insert_reading(initialized_db, "meter1", "2024-01-15T10:00:00", power_w=100.0)
    await insert_reading(initialized_db, "meter1", "2024-01-15T12:00:00", power_w=200.0)
    await insert_reading(initialized_db, "meter1", "2024-01-15T14:00:00", power_w=300.0)

    readings = await get_readings(
        initialized_db, "meter1", "2024-01-15T11:00:00", "2024-01-15T13:00:00"
    )
    assert len(readings) == 1
    assert readings[0]["power_w"] == 200.0


@pytest.mark.asyncio
async def test_get_readings_empty_range(initialized_db: str) -> None:
    await insert_reading(initialized_db, "meter1", "2024-01-15T10:00:00", power_w=100.0)
    readings = await get_readings(
        initialized_db, "meter1", "2025-01-01T00:00:00", "2025-01-02T00:00:00"
    )
    assert readings == []


@pytest.mark.asyncio
async def test_insert_and_get_mqtt_log(initialized_db: str) -> None:
    await insert_mqtt_log(
        initialized_db, "dev1", "ok", "538W, 42000.5kWh", "tele/dev1/SENSOR"
    )
    await insert_mqtt_log(
        initialized_db, "dev1", "invalid", "unparseable payload", "tele/dev1/SENSOR"
    )

    rows = await get_mqtt_log(initialized_db, limit=10)
    assert len(rows) == 2
    # Ordered by id DESC — most recent first
    assert rows[0]["status"] == "invalid"
    assert rows[1]["status"] == "ok"
    assert rows[1]["summary"] == "538W, 42000.5kWh"
    assert rows[0]["device_id"] == "dev1"
    assert rows[0]["topic"] == "tele/dev1/SENSOR"


@pytest.mark.asyncio
async def test_get_mqtt_log_respects_limit(initialized_db: str) -> None:
    for i in range(5):
        await insert_mqtt_log(initialized_db, "dev1", "ok", f"entry {i}", "t")
    rows = await get_mqtt_log(initialized_db, limit=3)
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_get_mqtt_log_empty(initialized_db: str) -> None:
    rows = await get_mqtt_log(initialized_db)
    assert rows == []


@pytest.mark.asyncio
async def test_mqtt_log_table_created(initialized_db: str) -> None:
    async with aiosqlite.connect(initialized_db) as db:
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='mqtt_log'"
        )
        row = await cursor.fetchone()
    assert row is not None
