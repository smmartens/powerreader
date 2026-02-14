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
