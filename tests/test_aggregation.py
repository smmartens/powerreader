import aiosqlite
import pytest

from powerreader.aggregation import (
    compute_daily_agg,
    compute_hourly_agg,
    get_avg_by_time_of_day,
    prune_raw_readings,
)
from powerreader.db import init_db, insert_reading


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
    assert row["min_power_w"] == 100.0
    assert row["max_power_w"] == 300.0
    assert row["avg_power_w"] == pytest.approx(200.0)
    # kWh = max(total_in) - min(total_in) = 1003.0 - 1000.0
    assert row["kwh_consumed"] == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_hourly_agg_kwh_delta(seeded_db: str) -> None:
    """Verify consumption is computed as max(total_in) - min(total_in)."""
    await compute_hourly_agg(seeded_db)

    async with aiosqlite.connect(seeded_db) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT kwh_consumed FROM hourly_agg WHERE hour = '2024-01-15T14'"
        )
        row = dict(await cursor.fetchone())

    # 1015.0 - 1010.0 = 5.0
    assert row["kwh_consumed"] == pytest.approx(5.0)


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
    # max across hourly maxes: max(300, 600)
    assert row["max_power_w"] == 600.0
    # min across hourly mins: min(100, 500)
    assert row["min_power_w"] == 100.0


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
    # Hour 10 has 2 hourly buckets: avg_power=200.0 (day1) and avg_power=200.0 (day2)
    assert 10 in hours
    assert 14 in hours
    assert len(result) == 2  # only hours 10 and 14 have data


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
