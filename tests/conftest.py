import json

import aiosqlite
import pytest
from fastapi.testclient import TestClient

from powerreader.config import Settings
from powerreader.db import init_db
from powerreader.main import app


@pytest.fixture
def test_settings() -> Settings:
    return Settings(db_path=":memory:", mqtt_host="localhost")


@pytest.fixture
def test_client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "test.db")


@pytest.fixture
async def initialized_db(db_path: str) -> str:
    await init_db(db_path)
    return db_path


@pytest.fixture
def sample_tasmota_payload() -> bytes:
    """A valid Tasmota LK13BE SENSOR payload."""
    return json.dumps(
        {
            "Time": "2024-01-15T14:30:00",
            "LK13BE": {
                "total": 42000.5,
                "total_out": 0,
                "current": 538,
                "voltage_l1": 230.1,
            },
        }
    ).encode()


@pytest.fixture
def sample_tasmota_payload_minimal() -> bytes:
    """A valid Tasmota payload with only Time and partial LK13BE fields."""
    return json.dumps(
        {
            "Time": "2024-01-15T14:35:00",
            "LK13BE": {"total": 42001.0},
        }
    ).encode()


@pytest.fixture
async def seeded_db(initialized_db: str) -> str:
    """DB with raw_readings spanning 2 hours across 2 days for aggregation tests."""
    from powerreader.db import insert_reading

    readings = [
        # Day 1, Hour 10 (3 readings)
        ("meter1", "2024-01-15T10:00:00", 1000.0, 0.0, 100.0, 230.0),
        ("meter1", "2024-01-15T10:20:00", 1001.0, 0.0, 200.0, 231.0),
        ("meter1", "2024-01-15T10:40:00", 1003.0, 0.0, 300.0, 229.0),
        # Day 1, Hour 14 (2 readings)
        ("meter1", "2024-01-15T14:00:00", 1010.0, 0.0, 500.0, 230.5),
        ("meter1", "2024-01-15T14:30:00", 1015.0, 0.0, 600.0, 230.0),
        # Day 2, Hour 10 (2 readings)
        ("meter1", "2024-01-16T10:00:00", 1100.0, 0.0, 150.0, 230.0),
        ("meter1", "2024-01-16T10:30:00", 1105.0, 0.0, 250.0, 231.0),
    ]
    for device_id, ts, total_in, total_out, power_w, voltage in readings:
        await insert_reading(
            initialized_db, device_id, ts, total_in, total_out, power_w, voltage
        )
    return initialized_db


@pytest.fixture
async def seeded_db_total_only(initialized_db: str) -> str:
    """DB with raw_readings that only have total_in (no power_w/voltage)."""
    from powerreader.db import insert_reading

    readings = [
        # Hour 10: total_in goes from 1000.0 to 1003.0 => 3 kWh delta
        ("meter1", "2024-01-15T10:00:00", 1000.0, 0.0, None, None),
        ("meter1", "2024-01-15T10:20:00", 1001.0, 0.0, None, None),
        ("meter1", "2024-01-15T10:40:00", 1003.0, 0.0, None, None),
    ]
    for device_id, ts, total_in, total_out, power_w, voltage in readings:
        await insert_reading(
            initialized_db, device_id, ts, total_in, total_out, power_w, voltage
        )
    return initialized_db


@pytest.fixture
async def api_db(seeded_db: str) -> str:
    """Seeded DB with hourly and daily aggregates pre-computed."""
    from powerreader.aggregation import compute_daily_agg, compute_hourly_agg

    await compute_hourly_agg(seeded_db)
    await compute_daily_agg(seeded_db)
    return seeded_db


@pytest.fixture
def api_client(api_db: str) -> TestClient:
    """TestClient with app.state.db_path pointing to the seeded test DB."""
    app.state.db_path = api_db
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
async def admin_db(initialized_db: str) -> str:
    """DB with a stuck/zero-consumption day and a spike hour for admin tests.

    Suspect day: 2024-02-01 — 3 raw readings all with total_in=500.0 (constant),
    daily_agg.kwh_consumed=0.

    Spike hour: 2024-02-02T14 — kwh_consumed=100.0 (far above 5× the 1.0 kWh
    normal hours), with 2 raw readings and a daily_agg row.
    """
    async with aiosqlite.connect(initialized_db) as db:
        # Suspect day raw readings (constant total_in)
        for ts in ("2024-02-01T10:00:00", "2024-02-01T10:20:00", "2024-02-01T10:40:00"):
            await db.execute(
                "INSERT INTO raw_readings (device_id, timestamp, total_in) VALUES (?, ?, ?)",
                ("meter1", ts, 500.0),
            )
        # Zero-consumption daily aggregate for the stuck day
        await db.execute(
            "INSERT INTO daily_agg (device_id, date, avg_power_w, kwh_consumed, reading_count)"
            " VALUES (?, ?, ?, ?, ?)",
            ("meter1", "2024-02-01", 0.0, 0.0, 3),
        )

        # Normal hours leading up to the spike (5 hours at 1.0 kWh each)
        for h in ("08", "09", "10", "11", "12"):
            await db.execute(
                "INSERT INTO hourly_agg (device_id, hour, avg_power_w, kwh_consumed, reading_count)"
                " VALUES (?, ?, ?, ?, ?)",
                ("meter1", f"2024-02-02T{h}", 1000.0, 1.0, 2),
            )
        # Spike hour raw readings
        for ts in ("2024-02-02T14:00:00", "2024-02-02T14:30:00"):
            await db.execute(
                "INSERT INTO raw_readings (device_id, timestamp, total_in) VALUES (?, ?, ?)",
                ("meter1", ts, 600.0),
            )
        # Spike hourly aggregate (100 kWh >> 5× avg ≈ MAX(1.0, 17.5*5)=87.5)
        await db.execute(
            "INSERT INTO hourly_agg (device_id, hour, avg_power_w, kwh_consumed, reading_count)"
            " VALUES (?, ?, ?, ?, ?)",
            ("meter1", "2024-02-02T14", 100000.0, 100.0, 2),
        )
        # Daily aggregate for the spike day (will be cleared on hour delete)
        await db.execute(
            "INSERT INTO daily_agg (device_id, date, avg_power_w, kwh_consumed, reading_count)"
            " VALUES (?, ?, ?, ?, ?)",
            ("meter1", "2024-02-02", 50000.0, 105.0, 12),
        )
        await db.commit()
    return initialized_db


@pytest.fixture
def admin_client(admin_db: str) -> TestClient:
    """TestClient pointing to the admin test DB."""
    app.state.db_path = admin_db
    return TestClient(app, raise_server_exceptions=False)
