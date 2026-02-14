import asyncio

from fastapi.testclient import TestClient

from powerreader.db import init_db
from powerreader.main import app


def _make_empty_client(tmp_path) -> TestClient:
    db_path = str(tmp_path / "empty.db")
    asyncio.run(init_db(db_path))
    app.state.db_path = db_path
    return TestClient(app, raise_server_exceptions=False)


class TestCurrentEndpoint:
    def test_returns_latest_reading(self, api_client):
        resp = api_client.get("/api/current?device_id=meter1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["device_id"] == "meter1"
        assert data["timestamp"] == "2024-01-16T10:30:00"

    def test_returns_404_when_empty(self, tmp_path):
        client = _make_empty_client(tmp_path)
        resp = client.get("/api/current?device_id=meter1")
        assert resp.status_code == 404


class TestHistoryEndpoint:
    def test_24h_returns_hourly_agg(self, api_client):
        resp = api_client.get("/api/history?device_id=meter1&range=24h")
        assert resp.status_code == 200
        body = resp.json()
        assert body["range"] == "24h"
        assert isinstance(body["data"], list)

    def test_7d_returns_hourly_agg(self, api_client):
        resp = api_client.get("/api/history?device_id=meter1&range=7d")
        assert resp.status_code == 200
        assert resp.json()["range"] == "7d"

    def test_30d_returns_daily_agg(self, api_client):
        resp = api_client.get("/api/history?device_id=meter1&range=30d")
        assert resp.status_code == 200
        body = resp.json()
        assert body["range"] == "30d"
        assert isinstance(body["data"], list)

    def test_invalid_range_returns_400(self, api_client):
        resp = api_client.get("/api/history?range=99d")
        assert resp.status_code == 400

    def test_defaults(self, api_client):
        resp = api_client.get("/api/history")
        assert resp.status_code == 200
        body = resp.json()
        assert body["range"] == "24h"


class TestAveragesEndpoint:
    def test_returns_hour_of_day_grouping(self, api_client):
        resp = api_client.get("/api/averages?device_id=meter1&days=1000")
        assert resp.status_code == 200
        body = resp.json()
        assert body["device_id"] == "meter1"
        assert body["days"] == 1000
        assert isinstance(body["data"], list)
        assert len(body["data"]) > 0
        assert "hour_of_day" in body["data"][0]
        assert "avg_power_w" in body["data"][0]

    def test_empty_db_returns_empty_data(self, tmp_path):
        client = _make_empty_client(tmp_path)
        resp = client.get("/api/averages?device_id=meter1")
        assert resp.status_code == 200
        assert resp.json()["data"] == []

    def test_defaults(self, api_client):
        resp = api_client.get("/api/averages")
        assert resp.status_code == 200
        body = resp.json()
        assert body["device_id"] == "meter1"
        assert body["days"] == 30


class TestDashboard:
    def test_returns_html(self, api_client):
        resp = api_client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Powerreader" in resp.text
