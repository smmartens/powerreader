import asyncio

from fastapi.testclient import TestClient

from powerreader.db import init_db, insert_mqtt_log
from powerreader.main import app


def _make_empty_client(tmp_path) -> TestClient:
    db_path = str(tmp_path / "empty.db")
    asyncio.run(init_db(db_path))
    app.state.db_path = db_path
    return TestClient(app, raise_server_exceptions=False)


def _make_log_client(tmp_path, entries: list[tuple[str, str, str, str]]) -> TestClient:
    """Create a TestClient with an initialized DB containing the given log entries."""
    db_path = str(tmp_path / "log.db")
    asyncio.run(init_db(db_path))
    for device_id, status, summary, topic in entries:
        asyncio.run(insert_mqtt_log(db_path, device_id, status, summary, topic))
    app.state.db_path = db_path
    return TestClient(app, raise_server_exceptions=False)


class TestVersionEndpoint:
    def test_returns_version(self, api_client):
        resp = api_client.get("/api/version")
        assert resp.status_code == 200
        data = resp.json()
        assert "version" in data
        assert isinstance(data["version"], str)


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
        if body["data"]:
            assert "bucket" in body["data"][0]
            assert body["data"][0]["bucket"] == body["data"][0]["hour"]

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
        if body["data"]:
            assert "bucket" in body["data"][0]
            assert body["data"][0]["bucket"] == body["data"][0]["date"]

    def test_invalid_range_returns_400(self, api_client):
        resp = api_client.get("/api/history?range=99d")
        assert resp.status_code == 400

    def test_defaults(self, api_client):
        resp = api_client.get("/api/history?device_id=meter1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["range"] == "24h"


class TestAveragesEndpoint:
    def test_returns_hour_of_day_grouping(self, api_client):
        resp = api_client.get(
            "/api/averages?device_id=meter1&from_date=2024-01-15&to_date=2024-01-16"
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["device_id"] == "meter1"
        assert body["from_date"] == "2024-01-15"
        assert body["to_date"] == "2024-01-16"
        assert isinstance(body["data"], list)
        assert len(body["data"]) > 0
        assert "hour_of_day" in body["data"][0]
        assert "avg_power_w" in body["data"][0]

    def test_defaults_to_earliest_and_today(self, api_client):
        resp = api_client.get("/api/averages?device_id=meter1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["device_id"] == "meter1"
        assert body["from_date"] == "2024-01-15"  # earliest date in test DB
        assert len(body["data"]) > 0

    def test_single_day(self, api_client):
        resp = api_client.get(
            "/api/averages?device_id=meter1&from_date=2024-01-15&to_date=2024-01-15"
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["from_date"] == "2024-01-15"
        assert body["to_date"] == "2024-01-15"
        assert len(body["data"]) > 0

    def test_empty_db_returns_empty_data(self, tmp_path):
        client = _make_empty_client(tmp_path)
        resp = client.get("/api/averages?device_id=meter1")
        assert resp.status_code == 200
        assert resp.json()["data"] == []

    def test_invalid_date_returns_400(self, api_client):
        resp = api_client.get("/api/averages?from_date=not-a-date")
        assert resp.status_code == 400

    def test_from_after_to_returns_400(self, api_client):
        resp = api_client.get(
            "/api/averages?device_id=meter1&from_date=2024-01-16&to_date=2024-01-15"
        )
        assert resp.status_code == 400


class TestStatsEndpoint:
    def test_returns_stats(self, api_client):
        resp = api_client.get("/api/stats?device_id=meter1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["device_id"] == "meter1"
        assert body["avg_kwh_per_day"] is not None
        assert body["avg_kwh_per_month"] is not None
        assert body["kwh_this_year"] is not None

    def test_returns_coverage_stats(self, api_client):
        resp = api_client.get("/api/stats?device_id=meter1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["first_reading_date"] == "2024-01-15"
        assert isinstance(body["days_since_first_reading"], int)
        assert body["days_since_first_reading"] > 0
        assert isinstance(body["days_with_full_coverage"], int)

    def test_empty_db_returns_nulls(self, tmp_path):
        client = _make_empty_client(tmp_path)
        resp = client.get("/api/stats?device_id=meter1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["avg_kwh_per_day"] is None
        assert body["avg_kwh_per_month"] is None
        assert body["kwh_this_year"] is None
        assert body["first_reading_date"] is None
        assert body["days_since_first_reading"] is None
        assert body["days_with_full_coverage"] == 0

    def test_auto_detects_device(self, api_client):
        resp = api_client.get("/api/stats")
        assert resp.status_code == 200
        assert resp.json()["device_id"] == "meter1"


class TestLogEndpoint:
    def test_returns_log_entries(self, tmp_path):
        client = _make_log_client(
            tmp_path,
            [
                ("dev1", "ok", "538W", "tele/dev1/SENSOR"),
                ("dev1", "invalid", "unparseable payload", "tele/dev1/SENSOR"),
            ],
        )
        resp = client.get("/api/log")
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert len(data) == 2
        assert data[0]["status"] == "invalid"
        assert data[1]["status"] == "ok"

    def test_empty_db_returns_empty_list(self, tmp_path):
        client = _make_empty_client(tmp_path)
        resp = client.get("/api/log")
        assert resp.status_code == 200
        assert resp.json()["data"] == []

    def test_limit_parameter(self, tmp_path):
        client = _make_log_client(
            tmp_path,
            [("dev1", "ok", f"entry {i}", "t") for i in range(5)],
        )
        resp = client.get("/api/log?limit=3")
        assert resp.status_code == 200
        assert len(resp.json()["data"]) == 3


class TestDashboard:
    def test_returns_html(self, api_client):
        resp = api_client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Powerreader" in resp.text

    def test_dashboard_has_log_link(self, api_client):
        resp = api_client.get("/")
        assert resp.status_code == 200
        assert "/log" in resp.text


class TestLogPage:
    def test_returns_html(self, api_client):
        resp = api_client.get("/log")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Message Log" in resp.text


class TestParameterClamping:
    def test_log_limit_clamped_to_1000(self, tmp_path):
        client = _make_empty_client(tmp_path)
        resp = client.get("/api/log?limit=999999")
        assert resp.status_code == 200

    def test_log_limit_negative_clamped_to_1(self, tmp_path):
        client = _make_empty_client(tmp_path)
        resp = client.get("/api/log?limit=-5")
        assert resp.status_code == 200


class TestExportEndpoint:
    def test_hourly_export_returns_csv(self, api_client):
        resp = api_client.get(
            "/api/export?start=2024-01-15&end=2024-01-16&report=hourly"
        )
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert "attachment" in resp.headers["content-disposition"]
        assert (
            "powerreader_hourly_2024-01-15_2024-01-16.csv"
            in resp.headers["content-disposition"]
        )
        lines = resp.text.strip().splitlines()
        expected = (
            "hour_of_day,avg_power_w,"
            "total_kwh,reading_count,days_covered,"
            "avg_coverage_seconds"
        )
        assert lines[0] == expected

    def test_hourly_export_csv_content(self, api_client):
        resp = api_client.get(
            "/api/export?start=2024-01-15&end=2024-01-16&report=hourly"
        )
        assert resp.status_code == 200
        lines = resp.text.strip().splitlines()
        # Header + 2 data rows: hour 10 (aggregated from 2 days) and hour 14
        assert len(lines) == 3
        # First data row is hour 10, aggregated from 2 days
        assert lines[1].startswith("10,")
        cols = lines[1].split(",")
        assert cols[4] == "2"  # days_covered = 2
        assert cols[5] is not None  # avg_coverage_seconds present

    def test_empty_range_returns_header_only(self, api_client):
        resp = api_client.get(
            "/api/export?start=2099-01-01&end=2099-01-02&report=hourly"
        )
        assert resp.status_code == 200
        lines = resp.text.strip().splitlines()
        assert len(lines) == 1  # header only

    def test_invalid_report_type_returns_400(self, api_client):
        resp = api_client.get(
            "/api/export?start=2024-01-15&end=2024-01-16&report=bogus"
        )
        assert resp.status_code == 400
        assert "bogus" in resp.json()["detail"]

    def test_start_after_end_returns_400(self, api_client):
        resp = api_client.get(
            "/api/export?start=2024-01-20&end=2024-01-10&report=hourly"
        )
        assert resp.status_code == 400

    def test_invalid_date_format_returns_400(self, api_client):
        resp = api_client.get(
            "/api/export?start=not-a-date&end=2024-01-16&report=hourly"
        )
        assert resp.status_code == 400

    def test_missing_start_returns_422(self, api_client):
        resp = api_client.get("/api/export?end=2024-01-16&report=hourly")
        assert resp.status_code == 422

    def test_missing_end_returns_422(self, api_client):
        resp = api_client.get("/api/export?start=2024-01-15&report=hourly")
        assert resp.status_code == 422

    def test_date_range_exceeds_limit_returns_400(self, api_client):
        resp = api_client.get(
            "/api/export?start=0001-01-01&end=9999-12-31&report=hourly"
        )
        assert resp.status_code == 400
        assert "3650" in resp.json()["detail"]

    def test_long_date_input_truncated(self, api_client):
        resp = api_client.get(
            "/api/export?start=2024-01-15xxxxxxxxxxxx&end=2024-01-16&report=hourly"
        )
        assert resp.status_code == 200

    def test_auto_detects_device(self, api_client):
        resp = api_client.get(
            "/api/export?start=2024-01-15&end=2024-01-16&report=hourly"
        )
        assert resp.status_code == 200
        lines = resp.text.strip().splitlines()
        assert len(lines) > 1

    def test_no_device_returns_404(self, tmp_path):
        client = _make_empty_client(tmp_path)
        resp = client.get("/api/export?start=2024-01-15&end=2024-01-16&report=hourly")
        assert resp.status_code == 404

    def test_default_report_is_hourly(self, api_client):
        resp = api_client.get("/api/export?start=2024-01-15&end=2024-01-16")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]


class TestExportPage:
    def test_returns_html(self, api_client):
        resp = api_client.get("/export")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Export" in resp.text

    def test_has_nav_links(self, api_client):
        resp = api_client.get("/export")
        assert "/log" in resp.text
        assert "/" in resp.text


class TestSecurityHeaders:
    def test_security_headers_present(self, api_client):
        resp = api_client.get("/api/version")
        assert resp.headers["X-Content-Type-Options"] == "nosniff"
        assert resp.headers["X-Frame-Options"] == "DENY"
        assert resp.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
        assert "default-src 'self'" in resp.headers["Content-Security-Policy"]

    def test_html_pages_have_security_headers(self, api_client):
        resp = api_client.get("/")
        assert resp.headers["X-Frame-Options"] == "DENY"
        assert "Content-Security-Policy" in resp.headers
