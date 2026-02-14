# powerreader

A self-hosted power consumption monitor that subscribes to MQTT messages from a Tasmota-flashed ESP32C3 connected to a smart power meter, stores readings in SQLite, computes consumption analytics, and serves a web dashboard — all in a single Docker container.

## Features

- Real-time power monitoring via MQTT (Tasmota SML/ENERGY format)
- SQLite storage with automatic hourly and daily aggregation
- Configurable raw data retention with automatic pruning
- REST API for current readings, history, and averages
- Web dashboard with Chart.js visualizations (24h/7d/30d views)
- Downsample mode to reduce storage (store every message or 1/min)
- Multi-meter ready via `device_id` column
- Single-container deployment with Docker

## Quick Start

1. Clone the repository and start the stack:
   ```bash
   git clone https://github.com/smmartens/powerreader.git
   cd powerreader
   docker compose up -d
   ```

2. Configure your Tasmota device to publish to the Mosquitto broker at `<host-ip>:1883`.

3. Open the dashboard at [http://localhost:8080](http://localhost:8080).

## Configuration

All configuration is via environment variables (set in `docker-compose.yml` or `docker run -e`):

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
| `FIELD_MAP` | `""` | Custom MQTT field mapping (comma-separated `key=path` pairs, e.g. `total_in=SML.Total_in,power_w=SML.Power_curr`). Empty = LK13BE defaults. |

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

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/` | Web dashboard |
| `GET` | `/api/current?device_id=meter1` | Latest reading for a device |
| `GET` | `/api/history?range=24h&device_id=meter1` | Time-series data (`24h`, `7d`, `30d`) |
| `GET` | `/api/averages?days=30&device_id=meter1` | Average power by hour of day |

## Development

```bash
# Install dependencies
uv sync

# Run locally
uv run uvicorn powerreader.main:app --reload --host 0.0.0.0 --port 8080

# Run tests (80% coverage gate enforced)
uv run pytest

# Lint and format
uv run ruff check .
uv run ruff format .

# Install pre-commit hooks (ruff + pytest)
uv run pre-commit install
```

## License

MIT
