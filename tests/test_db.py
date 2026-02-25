import aiosqlite
import pytest

from powerreader.db import (
    get_consumption_stats,
    get_coverage_stats,
    get_days_by_consumption,
    get_earliest_date,
    get_latest_reading,
    get_mqtt_log,
    get_readings,
    init_db,
    insert_mqtt_log,
    insert_reading,
    insert_reading_and_log,
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
async def test_insert_reading_and_log(initialized_db: str) -> None:
    """Both reading and log are written in a single transaction."""
    await insert_reading_and_log(
        initialized_db,
        device_id="meter1",
        timestamp="2024-01-15T10:00:00",
        total_in=1000.0,
        log_summary="1000.0kWh",
        log_topic="tele/meter1/SENSOR",
    )
    reading = await get_latest_reading(initialized_db, "meter1")
    assert reading is not None
    assert reading["total_in"] == 1000.0

    logs = await get_mqtt_log(initialized_db)
    assert len(logs) == 1
    assert logs[0]["status"] == "ok"
    assert logs[0]["summary"] == "1000.0kWh"


@pytest.mark.asyncio
async def test_get_days_by_consumption_highest(seeded_db: str) -> None:
    from powerreader.aggregation import compute_daily_agg, compute_hourly_agg

    await compute_hourly_agg(seeded_db)
    await compute_daily_agg(seeded_db)
    rows = await get_days_by_consumption(seeded_db, "meter1")
    assert len(rows) == 2
    assert rows[0]["date"] == "2024-01-15"  # 8.0 kWh — highest
    assert rows[1]["date"] == "2024-01-16"  # 5.0 kWh
    assert rows[0]["kwh_consumed"] > rows[1]["kwh_consumed"]


@pytest.mark.asyncio
async def test_get_days_by_consumption_lowest(seeded_db: str) -> None:
    from powerreader.aggregation import compute_daily_agg, compute_hourly_agg

    await compute_hourly_agg(seeded_db)
    await compute_daily_agg(seeded_db)
    rows = await get_days_by_consumption(seeded_db, "meter1", ascending=True)
    assert len(rows) == 2
    assert rows[0]["date"] == "2024-01-16"  # 5.0 kWh — lowest
    assert rows[1]["date"] == "2024-01-15"  # 8.0 kWh


@pytest.mark.asyncio
async def test_get_days_by_consumption_limit(seeded_db: str) -> None:
    from powerreader.aggregation import compute_daily_agg, compute_hourly_agg

    await compute_hourly_agg(seeded_db)
    await compute_daily_agg(seeded_db)
    rows = await get_days_by_consumption(seeded_db, "meter1", limit=1)
    assert len(rows) == 1
    assert rows[0]["date"] == "2024-01-15"


@pytest.mark.asyncio
async def test_get_days_by_consumption_empty(initialized_db: str) -> None:
    rows = await get_days_by_consumption(initialized_db, "meter1")
    assert rows == []


@pytest.mark.asyncio
async def test_get_coverage_stats_empty(initialized_db: str) -> None:
    result = await get_coverage_stats(initialized_db, "meter1")
    assert result["first_reading_date"] is None
    assert result["days_with_full_coverage"] == 0


@pytest.mark.asyncio
async def test_get_coverage_stats_first_date(seeded_db: str) -> None:
    from powerreader.aggregation import compute_hourly_agg

    await compute_hourly_agg(seeded_db)
    result = await get_coverage_stats(seeded_db, "meter1")
    assert result["first_reading_date"] == "2024-01-15"


@pytest.mark.asyncio
async def test_get_coverage_stats_counts_full_coverage_days(
    initialized_db: str,
) -> None:
    # Full day: all 24 hours with 3 readings each → qualifies
    for hour in range(24):
        for minute in [0, 20, 40]:
            ts = f"2024-03-01T{hour:02d}:{minute:02d}:00"
            base = 1000.0 + hour + minute / 100
            await insert_reading(initialized_db, "meter1", ts, base)
    # Partial day: only 2 hours present → does not qualify (COUNT(*) != 24)
    await insert_reading(initialized_db, "meter1", "2024-03-02T10:00:00", 2000.0)
    await insert_reading(initialized_db, "meter1", "2024-03-02T10:20:00", 2001.0)
    await insert_reading(initialized_db, "meter1", "2024-03-02T10:40:00", 2002.0)

    from powerreader.aggregation import compute_hourly_agg

    await compute_hourly_agg(initialized_db)
    result = await get_coverage_stats(initialized_db, "meter1")
    assert result["first_reading_date"] == "2024-03-01"
    assert result["days_with_full_coverage"] == 1  # only 2024-03-01 qualifies


@pytest.mark.asyncio
async def test_get_earliest_date(seeded_db: str) -> None:
    from powerreader.aggregation import compute_hourly_agg

    await compute_hourly_agg(seeded_db)
    result = await get_earliest_date(seeded_db, "meter1")
    assert result == "2024-01-15"


@pytest.mark.asyncio
async def test_get_earliest_date_empty(initialized_db: str) -> None:
    result = await get_earliest_date(initialized_db, "meter1")
    assert result is None


@pytest.mark.asyncio
async def test_consumption_stats(seeded_db: str) -> None:
    from powerreader.aggregation import compute_daily_agg, compute_hourly_agg

    await compute_hourly_agg(seeded_db)
    await compute_daily_agg(seeded_db)

    stats = await get_consumption_stats(seeded_db, "meter1", year=2024)
    assert stats["avg_kwh_per_day"] is not None
    assert stats["avg_kwh_per_month"] is not None
    # Year consumption: last total_in (1105.0) - first on/after 2024-01-01 (1000.0)
    assert stats["kwh_this_year"] == 105.0


@pytest.mark.asyncio
async def test_consumption_stats_empty(initialized_db: str) -> None:
    stats = await get_consumption_stats(initialized_db, "meter1", year=2024)
    assert stats["avg_kwh_per_day"] is None
    assert stats["avg_kwh_per_month"] is None
    assert stats["kwh_this_year"] is None


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
