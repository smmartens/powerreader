"""MQTT subscriber for Tasmota sensor messages."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import TYPE_CHECKING

import paho.mqtt.client as paho_mqtt

from powerreader.db import insert_mqtt_log, insert_reading_and_log

if TYPE_CHECKING:
    from powerreader.config import Settings

logger = logging.getLogger(__name__)

# Default field mapping for Tasmota LK13BE payloads.
DEFAULT_FIELD_MAP: dict[str, str] = {
    "total_in": "LK13BE.total",
    "total_out": "LK13BE.total_out",
    "power_w": "LK13BE.current",
    "voltage": "LK13BE.voltage_l1",
}


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

    result: dict[str, str | float | None] = {"timestamp": str(timestamp)}
    for key, path in field_map.items():
        result[key] = _resolve_dotted(data, path)

    return result


def extract_device_id(topic: str) -> str:
    """Extract the device ID from a topic like 'tele/<device_id>/SENSOR'."""
    parts = topic.split("/")
    if len(parts) >= 3:
        return parts[1]
    return topic


class MqttSubscriber:
    """Manages the MQTT connection and message handling."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._field_map = parse_field_map(settings.field_map)
        self._client = paho_mqtt.Client(
            callback_api_version=paho_mqtt.CallbackAPIVersion.VERSION2
        )
        if settings.mqtt_user:
            self._client.username_pw_set(settings.mqtt_user, settings.mqtt_pass)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_stored: dict[str, float] = {}

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

    def _on_message(
        self,
        client: paho_mqtt.Client,
        userdata: object,
        msg: paho_mqtt.MQTTMessage,
    ) -> None:
        try:
            parsed = parse_tasmota_message(msg.payload, field_map=self._field_map)
            if parsed is None:
                logger.debug("Skipping unparseable message on %s", msg.topic)
                device_id = extract_device_id(msg.topic)
                if self._loop is not None:
                    asyncio.run_coroutine_threadsafe(
                        insert_mqtt_log(
                            self._settings.db_path,
                            device_id,
                            "invalid",
                            "unparseable payload",
                            msg.topic,
                        ),
                        self._loop,
                    )
                return

            device_id = extract_device_id(msg.topic)
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
            summary = ", ".join(parts) if parts else None

            if self._loop is not None:
                asyncio.run_coroutine_threadsafe(
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
                    ),
                    self._loop,
                )
        except Exception as exc:
            logger.exception("Error processing message on %s", msg.topic)
            device_id = extract_device_id(msg.topic)
            if self._loop is not None:
                asyncio.run_coroutine_threadsafe(
                    insert_mqtt_log(
                        self._settings.db_path,
                        device_id,
                        "error",
                        str(exc),
                        msg.topic,
                    ),
                    self._loop,
                )

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Connect to broker and start the network loop in a background thread."""
        self._loop = loop
        self._client.connect(
            self._settings.mqtt_host,
            self._settings.mqtt_port,
        )
        self._client.loop_start()
        logger.info(
            "MQTT subscriber started (host=%s, topic=%s)",
            self._settings.mqtt_host,
            self._settings.mqtt_topic,
        )

    def stop(self) -> None:
        """Stop the network loop and disconnect."""
        self._client.loop_stop()
        self._client.disconnect()
        logger.info("MQTT subscriber stopped")
