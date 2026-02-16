"""MQTT subscriber for Tasmota sensor messages."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import ssl
import time
from typing import TYPE_CHECKING

import paho.mqtt.client as paho_mqtt

from powerreader.db import insert_mqtt_log, insert_reading_and_log

if TYPE_CHECKING:
    from powerreader.config import Settings

logger = logging.getLogger(__name__)

_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]", re.ASCII)


def _sanitize(value: str, max_len: int) -> str:
    """Truncate to *max_len* and strip control characters (except space)."""
    value = value[:max_len]
    return _CONTROL_CHAR_RE.sub("", value)


# Default field mapping for Tasmota LK13BE payloads.
DEFAULT_FIELD_MAP: dict[str, str] = {
    "total_in": "LK13BE.total",
    "total_out": "LK13BE.total_out",
    "power_w": "LK13BE.current",
    "voltage": "LK13BE.voltage_l1",
}


def parse_allowed_devices(raw: str) -> set[str]:
    """Parse a comma-separated list of allowed device IDs.

    Returns an empty set if the string is empty (meaning all devices
    are accepted).
    """
    if not raw.strip():
        return set()
    return {d.strip() for d in raw.split(",") if d.strip()}


def parse_field_map(raw: str) -> dict[str, str]:
    """Parse a comma-separated field mapping string.

    Format: "total_in=SML.Total_in,power_w=SML.Power_curr"
    Returns DEFAULT_FIELD_MAP if the string is empty.
    """
    if not raw.strip():
        return DEFAULT_FIELD_MAP
    result: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def _resolve_dotted(data: dict, path: str) -> float | None:
    """Resolve a dotted path like 'SML.Total_in' into a nested dict."""
    parts = path.split(".")
    current: dict | float | None = data
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    if current is None:
        return None
    try:
        val = float(current)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val):
        return None
    return val


def parse_tasmota_message(
    payload: bytes, field_map: dict[str, str] | None = None
) -> dict | None:
    """Parse a Tasmota SENSOR JSON payload.

    Returns a dict with keys: timestamp, total_in, total_out, power_w, voltage.
    Returns None if the payload cannot be parsed.
    """
    if field_map is None:
        field_map = DEFAULT_FIELD_MAP
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if not isinstance(data, dict):
        return None

    timestamp = data.get("Time")
    if timestamp is None:
        return None

    timestamp = _sanitize(str(timestamp), 32)
    if not _TIMESTAMP_RE.match(timestamp):
        return None

    result: dict[str, str | float | None] = {"timestamp": timestamp}
    for key, path in field_map.items():
        result[key] = _resolve_dotted(data, path)

    return result


def extract_device_id(topic: str) -> str:
    """Extract the device ID from a topic like 'tele/<device_id>/SENSOR'."""
    topic = _sanitize(topic, 256)
    parts = topic.split("/")
    raw = parts[1] if len(parts) >= 3 else topic
    return _sanitize(raw, 64)


class MqttSubscriber:
    """Manages the MQTT connection and message handling."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._field_map = parse_field_map(settings.field_map)
        self._allowed_devices = parse_allowed_devices(settings.allowed_devices)
        self._client = paho_mqtt.Client(
            callback_api_version=paho_mqtt.CallbackAPIVersion.VERSION2
        )
        if settings.mqtt_user:
            self._client.username_pw_set(settings.mqtt_user, settings.mqtt_pass)
        if settings.mqtt_tls:
            ca_certs = settings.mqtt_tls_ca or None
            self._client.tls_set(
                ca_certs=ca_certs,
                tls_version=ssl.PROTOCOL_TLS_CLIENT,
            )
        self._client.reconnect_delay_set(min_delay=1, max_delay=60)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_stored: dict[str, float] = {}

    def _async_execute(self, coro: object) -> None:
        """Schedule a coroutine on the event loop from a callback thread."""
        if self._loop is not None:
            asyncio.run_coroutine_threadsafe(coro, self._loop)  # type: ignore[arg-type]

    def _log_event(self, status: str, summary: str) -> None:
        """Insert an MQTT log entry from a callback thread."""
        self._async_execute(
            insert_mqtt_log(self._settings.db_path, None, status, summary, None)
        )

    def _on_connect(
        self,
        client: paho_mqtt.Client,
        userdata: object,
        flags: paho_mqtt.ConnectFlags,
        rc: paho_mqtt.ReasonCode,
        properties: paho_mqtt.Properties | None = None,
    ) -> None:
        logger.info("Connected to MQTT broker (rc=%s)", rc)
        client.subscribe(self._settings.mqtt_topic)
        self._log_event("ok", f"connected to broker (rc={rc})")

    def _on_disconnect(
        self,
        client: paho_mqtt.Client,
        userdata: object,
        disconnect_flags: paho_mqtt.DisconnectFlags,
        rc: paho_mqtt.ReasonCode,
        properties: paho_mqtt.Properties | None = None,
    ) -> None:
        logger.warning("Disconnected from MQTT broker (rc=%s)", rc)
        self._log_event("error", f"disconnected from broker (rc={rc})")

    def _on_message(
        self,
        client: paho_mqtt.Client,
        userdata: object,
        msg: paho_mqtt.MQTTMessage,
    ) -> None:
        try:
            device_id = extract_device_id(msg.topic)

            # Allowlist check — drop messages from unknown devices
            if self._allowed_devices and device_id not in self._allowed_devices:
                logger.debug("Ignoring device %s (not in allowlist)", device_id)
                return

            parsed = parse_tasmota_message(msg.payload, field_map=self._field_map)
            if parsed is None:
                logger.debug("Skipping unparseable message on %s", msg.topic)
                self._async_execute(
                    insert_mqtt_log(
                        self._settings.db_path,
                        device_id,
                        "invalid",
                        "unparseable payload",
                        msg.topic,
                    )
                )
                return
            now = time.monotonic()

            # Downsample check
            if self._settings.poll_store_mode == "downsample_60s":
                last = self._last_stored.get(device_id)
                if last is not None and now - last < 60.0:
                    return

            self._last_stored[device_id] = now

            # Build summary from parsed values
            parts: list[str] = []
            if parsed.get("power_w") is not None:
                parts.append(f"{parsed['power_w']}W")
            if parsed.get("total_in") is not None:
                parts.append(f"{parsed['total_in']}kWh")
            if parsed.get("voltage") is not None:
                parts.append(f"{parsed['voltage']}V")
            summary = _sanitize(", ".join(parts), 256) if parts else None

            self._async_execute(
                insert_reading_and_log(
                    db_path=self._settings.db_path,
                    device_id=device_id,
                    timestamp=parsed["timestamp"],
                    total_in=parsed.get("total_in"),
                    total_out=parsed.get("total_out"),
                    power_w=parsed.get("power_w"),
                    voltage=parsed.get("voltage"),
                    log_summary=summary,
                    log_topic=msg.topic,
                )
            )
        except Exception as exc:
            logger.exception("Error processing message on %s", msg.topic)
            device_id = extract_device_id(msg.topic)
            self._async_execute(
                insert_mqtt_log(
                    self._settings.db_path,
                    device_id,
                    "error",
                    str(exc),
                    msg.topic,
                )
            )

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Connect to broker and start the network loop in a background thread.

        If the broker is unreachable, paho-mqtt will keep retrying
        automatically via ``loop_start`` — the app stays up.
        """
        self._loop = loop
        self._client.loop_start()
        try:
            self._client.connect(
                self._settings.mqtt_host,
                self._settings.mqtt_port,
            )
            logger.info(
                "MQTT subscriber started (host=%s, topic=%s)",
                self._settings.mqtt_host,
                self._settings.mqtt_topic,
            )
        except OSError:
            logger.warning(
                "MQTT broker unreachable (host=%s, port=%s), "
                "will keep retrying in the background",
                self._settings.mqtt_host,
                self._settings.mqtt_port,
            )

    def stop(self) -> None:
        """Stop the network loop and disconnect."""
        self._client.loop_stop()
        self._client.disconnect()
        logger.info("MQTT subscriber stopped")
