import json

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
    """A valid Tasmota SML SENSOR payload."""
    return json.dumps(
        {
            "Time": "2024-01-15T14:30:00",
            "SML": {
                "Total_in": 42000.5,
                "Total_out": 0,
                "Power_curr": 538,
                "Volt_p1": 230.1,
            },
        }
    ).encode()


@pytest.fixture
def sample_tasmota_payload_minimal() -> bytes:
    """A valid Tasmota payload with only Time and partial SML fields."""
    return json.dumps(
        {
            "Time": "2024-01-15T14:35:00",
            "SML": {"Total_in": 42001.0},
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
