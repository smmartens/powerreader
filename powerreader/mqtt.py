"""MQTT subscriber for Tasmota sensor messages."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING

import paho.mqtt.client as paho_mqtt

from powerreader.db import insert_reading

if TYPE_CHECKING:
    from powerreader.config import Settings

logger = logging.getLogger(__name__)

# Default field mapping for Tasmota SML payloads.
DEFAULT_FIELD_MAP: dict[str, str] = {
    "total_in": "SML.Total_in",
    "total_out": "SML.Total_out",
    "power_w": "SML.Power_curr",
    "voltage": "SML.Volt_p1",
}


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
        return float(current)
    except (TypeError, ValueError):
        return None


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
        parsed = parse_tasmota_message(msg.payload)
        if parsed is None:
            logger.debug("Skipping unparseable message on %s", msg.topic)
            return

        device_id = extract_device_id(msg.topic)
        now = time.monotonic()

        # Downsample check
        if self._settings.poll_store_mode == "downsample_60s":
            last = self._last_stored.get(device_id, 0.0)
            if now - last < 60.0:
                return

        self._last_stored[device_id] = now

        if self._loop is not None:
            asyncio.run_coroutine_threadsafe(
                insert_reading(
                    db_path=self._settings.db_path,
                    device_id=device_id,
                    timestamp=parsed["timestamp"],
                    total_in=parsed.get("total_in"),
                    total_out=parsed.get("total_out"),
                    power_w=parsed.get("power_w"),
                    voltage=parsed.get("voltage"),
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
