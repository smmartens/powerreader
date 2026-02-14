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
