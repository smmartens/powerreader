# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**powerreader** — A self-hosted power consumption monitor that runs as a Docker container on local networks (e.g. NAS devices). It subscribes to MQTT messages from a Tasmota-flashed ESP32C3 connected to a smart power meter, stores readings, computes consumption analytics, and serves a web dashboard.

Repository: https://github.com/smmartens/powerreader.git

## Architecture

```
┌──────────────┐   MQTT    ┌─────────────────────────────────────────┐
│ Tasmota       │──────────▶│  powerreader (Docker container)         │
│ ESP32C3       │           │                                         │
│ (smart meter) │           │  ┌─────────────┐   ┌────────────────┐  │
└──────────────┘           │  │ MQTT        │──▶│ SQLite DB      │  │
                            │  │ Subscriber  │   │                │  │
                            │  └─────────────┘   │ - raw_readings │  │
                            │                     │ - hourly_agg   │  │
                            │  ┌─────────────┐   │ - daily_agg    │  │
                            │  │ Aggregation │──▶│                │  │
                            │  │ Scheduler   │   └───────┬────────┘  │
                            │  └─────────────┘           │           │
                            │                     ┌──────┴────────┐  │
                            │  ┌─────────────┐   │ FastAPI       │  │
                            │  │ Web UI      │◀──│ + Chart.js    │  │
                            │  │ :8080       │   └───────────────┘  │
                            │  └─────────────┘                       │
                            └─────────────────────────────────────────┘
```

### Components

- **MQTT Subscriber** — Connects to a broker on the local network, subscribes to Tasmota `tele/<topic>/SENSOR` messages. Parses JSON payloads (SML/ENERGY format). Stores raw readings in SQLite. Configurable storage granularity (store every message or downsample).
- **SQLite Database** — Single-file DB inside the container (volume-mounted for persistence). Tables: `raw_readings` (full-resolution, retained ~30 days), `hourly_agg`, `daily_agg` (retained indefinitely). Data model uses a `device_id` column to allow future multi-meter support.
- **Aggregation Scheduler** — Background jobs that compute hourly/daily aggregates from raw data and prune expired raw readings based on configurable retention.
- **FastAPI Web Server** — Serves a JSON REST API and a minimal server-rendered HTML dashboard with Chart.js for visualization. Exposes current reading, consumption over time, and average-by-time-of-day charts.

### Tasmota MQTT Format

The Tasmota device publishes to `tele/<topic>/SENSOR` at its configured `TelePeriod` (default 300s). Payload example:
```json
{"Time":"2024-01-15T14:30:00","SML":{"Total_in":42000.5,"Total_out":0,"Power_curr":538,"Volt_p1":230.1}}
```
Field names depend on the Tasmota meter script. The app must handle configurable JSON field mapping.

## Tech Stack

- **Python 3.12+**
- **FastAPI** — web framework and REST API
- **paho-mqtt** — MQTT client
- **SQLite** (via `aiosqlite`) — storage, no extra services needed
- **APScheduler** — background aggregation/pruning jobs
- **Chart.js** — client-side charting (served as static asset)
- **Jinja2** — HTML templates for dashboard pages
- **Docker** — single-container deployment
- **UV** — Python package/project manager
- **pytest + pytest-cov** — testing with coverage enforcement
- **pre-commit** — local hooks for lint + tests
- **ruff** — linter and formatter

## Build & Run Commands

```bash
# Development
uv sync                          # Install dependencies
uv run uvicorn powerreader.main:app --reload --host 0.0.0.0 --port 8080

# Docker
docker build -t powerreader .
docker run -d -p 8080:8080 -v powerreader_data:/data -e MQTT_HOST=<broker_ip> powerreader

# Tests
uv run pytest                    # All tests
uv run pytest tests/test_mqtt.py # Single test file
uv run pytest -k "test_name"     # Single test by name
uv run pytest --cov=powerreader --cov-fail-under=80  # With coverage gate

# Lint
uv run ruff check .
uv run ruff format .

# Pre-commit (runs ruff + pytest automatically before each commit)
uv run pre-commit install        # One-time setup
uv run pre-commit run --all-files # Manual run
```

## Testing Strategy

- **Framework:** pytest with pytest-cov. Minimum **80% coverage** enforced (`--cov-fail-under=80`).
- **Pre-commit hooks:** ruff (check + format) and pytest run locally before every commit via the `pre-commit` framework.
- **CI:** GitHub Actions runs the same checks (ruff + pytest with coverage gate) on every push/PR.
- **Unit tests use mocks/fixtures — no external services required:**
  - **MQTT:** Mock the paho-mqtt client. Test message parsing with sample Tasmota JSON payloads as fixtures.
  - **Database:** Use an in-memory SQLite (`":memory:"`) per test. Shared fixtures create schema and seed test data.
  - **API:** Use FastAPI's `TestClient` with the DB overridden to in-memory SQLite.
  - **Aggregation:** Pure functions tested with known input/output datasets.
- **Test file naming:** mirror source files — `powerreader/mqtt.py` → `tests/test_mqtt.py`.
- **Fixtures** live in `tests/conftest.py` (DB sessions, sample MQTT payloads, FastAPI test client).

## Backup & Recovery

All state is a single SQLite file at `DB_PATH` (default `/data/powerreader.db`), volume-mounted out of the container. Rely on NAS-level volume snapshots/backups. Recovery: stop container → replace DB file with backup → start container. Schema is auto-created on startup, so a fresh start with no DB file is also safe (just loses history). Optionally add a `VACUUM INTO` scheduled backup in a later phase.

## Configuration

All config via environment variables (Docker-friendly):

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | MQTT broker address |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USER` | `""` | MQTT username |
| `MQTT_PASS` | `""` | MQTT password |
| `MQTT_TOPIC` | `tele/+/SENSOR` | MQTT topic to subscribe |
| `DB_PATH` | `/data/powerreader.db` | SQLite database path |
| `POLL_STORE_MODE` | `all` | `all` = store every message, `downsample_60s` = 1/min |
| `RAW_RETENTION_DAYS` | `30` | Days to keep raw readings |
| `WEB_PORT` | `8080` | Dashboard port |

## Project Directory Structure (Target)

```
powerreader/
├── CLAUDE.md
├── Dockerfile
├── pyproject.toml
├── README.md
├── powerreader/
│   ├── __init__.py
│   ├── main.py              # FastAPI app + startup (MQTT, scheduler)
│   ├── config.py            # Pydantic settings from env vars
│   ├── mqtt.py              # MQTT subscriber + message parsing
│   ├── db.py                # SQLite schema, read/write helpers
│   ├── aggregation.py       # Hourly/daily aggregation + pruning logic
│   ├── api.py               # REST API routes (JSON endpoints)
│   ├── templates/
│   │   └── dashboard.html   # Jinja2 template with Chart.js
│   └── static/
│       └── chart.min.js     # Chart.js (vendored or CDN)
└── tests/
    ├── test_mqtt.py
    ├── test_db.py
    ├── test_aggregation.py
    └── test_api.py
```

## Implementation Plan

### Phase 1: Project Skeleton
- [ ] 1. Set up `pyproject.toml` with UV, declare dependencies (fastapi, uvicorn, paho-mqtt, aiosqlite, apscheduler, jinja2, ruff, pytest, pytest-cov, pre-commit)
- [ ] 2. Configure ruff in `pyproject.toml` and set up `.pre-commit-config.yaml` (ruff + pytest hooks)
- [ ] 3. Create `tests/conftest.py` with initial shared fixtures
- [ ] 4. Create `powerreader/config.py` — Pydantic `Settings` class reading env vars
- [ ] 5. Create `powerreader/main.py` — minimal FastAPI app with health endpoint
- [ ] 6. Create `Dockerfile` (Python 3.12 slim, UV install, volume at `/data`)
- [ ] 7. Verify: `docker build` + `docker run` + hit health endpoint

### Phase 2: Database Layer
- [ ] 6. Create `powerreader/db.py` — SQLite schema (raw_readings, hourly_agg, daily_agg) with device_id column
- [ ] 7. Auto-create tables on startup
- [ ] 8. Write insert/query helpers for raw readings
- [ ] 9. Tests for DB layer

### Phase 3: MQTT Subscriber
- [ ] 10. Create `powerreader/mqtt.py` — connect to broker, subscribe to topic, parse Tasmota JSON
- [ ] 11. Configurable field mapping (which JSON keys = total_kwh, power_w, etc.)
- [ ] 12. Downsample logic (store all vs. 1/min)
- [ ] 13. Start MQTT client as background task on FastAPI startup
- [ ] 14. Tests with mocked MQTT messages

### Phase 4: Aggregation & Retention
- [ ] 15. Create `powerreader/aggregation.py` — compute hourly/daily averages from raw data
- [ ] 16. Calculate consumption deltas (kWh difference between readings)
- [ ] 17. Average-by-time-of-day calculation (e.g. "average power at 11:00")
- [ ] 18. Scheduled job to prune raw readings older than `RAW_RETENTION_DAYS`
- [ ] 19. Wire APScheduler into FastAPI lifespan
- [ ] 20. Tests for aggregation math

### Phase 5: REST API
- [ ] 21. Create `powerreader/api.py` — endpoints:
  - `GET /api/current` — latest reading
  - `GET /api/history?range=24h|7d|30d` — time-series data
  - `GET /api/averages?granularity=hourly` — avg by time-of-day
- [ ] 22. Tests for API endpoints

### Phase 6: Web Dashboard
- [ ] 23. Create `powerreader/templates/dashboard.html` — single-page dashboard
- [ ] 24. Current power + meter reading display
- [ ] 25. Line chart: consumption over time (24h / 7d / 30d toggle)
- [ ] 26. Bar chart: average consumption by hour-of-day
- [ ] 27. Auto-refresh via periodic fetch

### Phase 7: Polish & Deploy
- [ ] 28. Docker Compose example with Mosquitto broker
- [ ] 29. README with setup instructions
- [ ] 30. CI: GitHub Actions for lint + tests
