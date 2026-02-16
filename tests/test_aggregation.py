import aiosqlite
import pytest

from powerreader.aggregation import (
    compute_daily_agg,
    compute_hourly_agg,
    get_avg_by_time_of_day,
    prune_mqtt_log,
    prune_raw_readings,
)
from powerreader.db import init_db, insert_mqtt_log, insert_reading


@pytest.mark.asyncio
async def test_hourly_agg_computes_correctly(seeded_db: str) -> None:
    count = await compute_hourly_agg(seeded_db)
    assert count >= 3  # 3 distinct hour groups

    async with aiosqlite.connect(seeded_db) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM hourly_agg"
            " WHERE hour = '2024-01-15T10' AND device_id = 'meter1'"
        )
        row = dict(await cursor.fetchone())

    assert row["reading_count"] == 3
    # delta kWh = 1003.0 - 1000.0 = 3.0, avg_power_w = 3.0 * 1000 = 3000.0
    assert row["avg_power_w"] == pytest.approx(3000.0)
    assert row["kwh_consumed"] == pytest.approx(3.0)
    # Readings at T10:00, T10:20, T10:40 → coverage = 40*60 = 2400s
    assert row["coverage_seconds"] == 2400


@pytest.mark.asyncio
async def test_hourly_agg_kwh_delta(seeded_db: str) -> None:
    """Verify consumption is computed as max(total_in) - min(total_in)."""
    await compute_hourly_agg(seeded_db)

    async with aiosqlite.connect(seeded_db) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT kwh_consumed, coverage_seconds FROM hourly_agg"
            " WHERE hour = '2024-01-15T14'"
        )
        row = dict(await cursor.fetchone())

    # 1015.0 - 1010.0 = 5.0
    assert row["kwh_consumed"] == pytest.approx(5.0)
    # Readings at T14:00, T14:30 → coverage = 30*60 = 1800s
    assert row["coverage_seconds"] == 1800


@pytest.mark.asyncio
async def test_daily_agg_computes_correctly(seeded_db: str) -> None:
    await compute_hourly_agg(seeded_db)
    count = await compute_daily_agg(seeded_db)
    assert count >= 2  # 2 distinct days

    async with aiosqlite.connect(seeded_db) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM daily_agg WHERE date = '2024-01-15' AND device_id = 'meter1'"
        )
        row = dict(await cursor.fetchone())

    # Day 1 has 2 hourly buckets: hour 10 (3 readings) + hour 14 (2 readings)
    assert row["reading_count"] == 5
    # kwh = 3.0 (hour 10) + 5.0 (hour 14)
    assert row["kwh_consumed"] == pytest.approx(8.0)
    # avg_power_w = AVG(3000.0, 5000.0) = 4000.0
    assert row["avg_power_w"] == pytest.approx(4000.0)


@pytest.mark.asyncio
async def test_prune_deletes_old_readings(db_path: str) -> None:
    await init_db(db_path)
    # Insert an old reading and a recent one
    await insert_reading(db_path, "meter1", "2020-01-01T00:00:00", power_w=100.0)
    await insert_reading(db_path, "meter1", "2099-01-01T00:00:00", power_w=200.0)

    deleted = await prune_raw_readings(db_path, retention_days=30)
    assert deleted == 1

    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM raw_readings")
        (remaining,) = await cursor.fetchone()
    assert remaining == 1


@pytest.mark.asyncio
async def test_prune_keeps_recent_readings(seeded_db: str) -> None:
    # All seeded readings are from 2024, which is in the past relative to now,
    # but let's use a huge retention to keep them all
    deleted = await prune_raw_readings(seeded_db, retention_days=999999)
    assert deleted == 0


@pytest.mark.asyncio
async def test_avg_by_time_of_day(seeded_db: str) -> None:
    await compute_hourly_agg(seeded_db)

    # Use a large day window since test data is from 2024
    result = await get_avg_by_time_of_day(seeded_db, "meter1", days=999999)

    hours = {r["hour_of_day"]: r["avg_power_w"] for r in result}
    # Hour 10: day1 delta=3kWh→3000W, day2 delta=5kWh→5000W, AVG=4000
    assert hours[10] == pytest.approx(4000.0)
    # Hour 14: delta=5kWh→5000W
    assert hours[14] == pytest.approx(5000.0)
    assert len(result) == 2  # only hours 10 and 14 have data


@pytest.mark.asyncio
async def test_hourly_agg_total_in_only(seeded_db_total_only: str) -> None:
    """Aggregation derives avg_power_w from kWh delta when power_w is NULL."""
    count = await compute_hourly_agg(seeded_db_total_only)
    assert count == 1

    async with aiosqlite.connect(seeded_db_total_only) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM hourly_agg"
            " WHERE hour = '2024-01-15T10' AND device_id = 'meter1'"
        )
        row = dict(await cursor.fetchone())

    assert row["reading_count"] == 3
    # delta kWh = 1003.0 - 1000.0 = 3.0, avg_power_w = 3.0 * 1000 = 3000.0
    assert row["avg_power_w"] == pytest.approx(3000.0)
    assert row["kwh_consumed"] == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_hourly_agg_mixed(initialized_db: str) -> None:
    """Even when power_w is present, avg_power_w is derived from total_in delta."""
    readings = [
        # 2 readings with power_w, 1 without
        ("meter1", "2024-01-15T10:00:00", 1000.0, 0.0, 100.0, 230.0),
        ("meter1", "2024-01-15T10:20:00", 1001.0, 0.0, 200.0, 231.0),
        ("meter1", "2024-01-15T10:40:00", 1003.0, 0.0, None, None),
    ]
    for device_id, ts, total_in, total_out, power_w, voltage in readings:
        await insert_reading(
            initialized_db, device_id, ts, total_in, total_out, power_w, voltage
        )

    await compute_hourly_agg(initialized_db)

    async with aiosqlite.connect(initialized_db) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM hourly_agg"
            " WHERE hour = '2024-01-15T10' AND device_id = 'meter1'"
        )
        row = dict(await cursor.fetchone())

    # delta kWh = 1003.0 - 1000.0 = 3.0, avg_power_w = 3.0 * 1000 = 3000.0
    assert row["avg_power_w"] == pytest.approx(3000.0)
    assert row["kwh_consumed"] == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_hourly_agg_idempotent(seeded_db: str) -> None:
    """Running aggregation twice should not create duplicates."""
    await compute_hourly_agg(seeded_db)
    await compute_hourly_agg(seeded_db)

    async with aiosqlite.connect(seeded_db) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM hourly_agg")
        (count,) = await cursor.fetchone()
    # Should still be exactly 3 hourly groups (not 6)
    assert count == 3


@pytest.mark.asyncio
async def test_daily_agg_idempotent(seeded_db: str) -> None:
    """Running daily aggregation twice should not create duplicates."""
    await compute_hourly_agg(seeded_db)
    await compute_daily_agg(seeded_db)
    await compute_daily_agg(seeded_db)

    async with aiosqlite.connect(seeded_db) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM daily_agg")
        (count,) = await cursor.fetchone()
    assert count == 2


@pytest.mark.asyncio
async def test_avg_by_time_of_day_empty(initialized_db: str) -> None:
    result = await get_avg_by_time_of_day(initialized_db, "meter1")
    assert result == []


@pytest.mark.asyncio
async def test_hourly_agg_single_reading_coverage_zero(initialized_db: str) -> None:
    """A single reading in a bucket should have coverage_seconds = 0."""
    await insert_reading(
        initialized_db, "meter1", "2024-01-15T12:00:00", 2000.0, 0.0, 400.0, 230.0
    )
    await compute_hourly_agg(initialized_db)

    async with aiosqlite.connect(initialized_db) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT coverage_seconds FROM hourly_agg"
            " WHERE hour = '2024-01-15T12' AND device_id = 'meter1'"
        )
        row = dict(await cursor.fetchone())

    assert row["coverage_seconds"] == 0


@pytest.mark.asyncio
async def test_prune_mqtt_log_deletes_old(db_path: str) -> None:
    await init_db(db_path)
    # Insert an old entry by manually setting timestamp
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            "INSERT INTO mqtt_log (timestamp, device_id, status, summary, topic)"
            " VALUES ('2020-01-01T00:00:00', 'dev1', 'ok', 'old', 't')"
        )
        await db.commit()
    # Insert a recent entry (uses default datetime('now'))
    await insert_mqtt_log(db_path, "dev1", "ok", "recent", "t")

    deleted = await prune_mqtt_log(db_path, retention_days=30)
    assert deleted == 1

    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM mqtt_log")
        (remaining,) = await cursor.fetchone()
    assert remaining == 1


@pytest.mark.asyncio
async def test_prune_mqtt_log_keeps_recent(db_path: str) -> None:
    await init_db(db_path)
    await insert_mqtt_log(db_path, "dev1", "ok", "recent", "t")
    deleted = await prune_mqtt_log(db_path, retention_days=30)
    assert deleted == 0
