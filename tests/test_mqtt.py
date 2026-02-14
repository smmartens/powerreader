"""Tests for the MQTT subscriber and message parsing."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

from powerreader.config import Settings
from powerreader.mqtt import MqttSubscriber, extract_device_id, parse_tasmota_message


class TestParseTasmotaMessage:
    def test_valid_sml_payload(self, sample_tasmota_payload: bytes) -> None:
        result = parse_tasmota_message(sample_tasmota_payload)
        assert result is not None
        assert result["timestamp"] == "2024-01-15T14:30:00"
        assert result["total_in"] == 42000.5
        assert result["total_out"] == 0.0
        assert result["power_w"] == 538.0
        assert result["voltage"] == 230.1

    def test_minimal_payload(self, sample_tasmota_payload_minimal: bytes) -> None:
        result = parse_tasmota_message(sample_tasmota_payload_minimal)
        assert result is not None
        assert result["timestamp"] == "2024-01-15T14:35:00"
        assert result["total_in"] == 42001.0
        assert result["total_out"] is None
        assert result["power_w"] is None
        assert result["voltage"] is None

    def test_invalid_json(self) -> None:
        assert parse_tasmota_message(b"not json") is None

    def test_empty_payload(self) -> None:
        assert parse_tasmota_message(b"") is None

    def test_missing_time_field(self) -> None:
        assert parse_tasmota_message(b'{"SML": {"Total_in": 1}}') is None

    def test_non_dict_payload(self) -> None:
        assert parse_tasmota_message(b"[1, 2, 3]") is None

    def test_custom_field_map(self, sample_tasmota_payload: bytes) -> None:
        custom = {"power_w": "SML.Power_curr"}
        result = parse_tasmota_message(sample_tasmota_payload, field_map=custom)
        assert result is not None
        assert result["power_w"] == 538.0
        assert "total_in" not in result


class TestExtractDeviceId:
    def test_standard_topic(self) -> None:
        assert extract_device_id("tele/tasmota_ABC123/SENSOR") == "tasmota_ABC123"

    def test_nested_topic(self) -> None:
        assert extract_device_id("tele/mydevice/SENSOR") == "mydevice"

    def test_short_topic_fallback(self) -> None:
        assert extract_device_id("short") == "short"

    def test_extra_segments(self) -> None:
        assert extract_device_id("tele/dev/SENSOR/extra") == "dev"


class TestMqttSubscriberDownsample:
    def _make_subscriber(self, mode: str = "all") -> MqttSubscriber:
        settings = Settings(
            db_path=":memory:",
            mqtt_host="localhost",
            poll_store_mode=mode,
        )
        with patch("powerreader.mqtt.paho_mqtt.Client"):
            return MqttSubscriber(settings)

    def _make_message(self, topic: str, payload: bytes) -> MagicMock:
        msg = MagicMock()
        msg.topic = topic
        msg.payload = payload
        return msg

    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_store_all_mode(
        self, mock_insert: AsyncMock, sample_tasmota_payload: bytes
    ) -> None:
        subscriber = self._make_subscriber("all")
        loop = asyncio.new_event_loop()
        subscriber._loop = loop

        msg = self._make_message("tele/dev1/SENSOR", sample_tasmota_payload)

        try:
            subscriber._on_message(subscriber._client, None, msg)
            loop.run_until_complete(asyncio.sleep(0.05))
        finally:
            loop.close()

        mock_insert.assert_called_once()
        call_kwargs = mock_insert.call_args
        assert call_kwargs.kwargs["device_id"] == "dev1"
        assert call_kwargs.kwargs["timestamp"] == "2024-01-15T14:30:00"
        assert call_kwargs.kwargs["power_w"] == 538.0

    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_downsample_skips_within_60s(
        self, mock_insert: AsyncMock, sample_tasmota_payload: bytes
    ) -> None:
        subscriber = self._make_subscriber("downsample_60s")
        loop = asyncio.new_event_loop()
        subscriber._loop = loop

        msg = self._make_message("tele/dev1/SENSOR", sample_tasmota_payload)

        try:
            # First message: stored
            subscriber._on_message(subscriber._client, None, msg)
            # Second message immediately: skipped
            subscriber._on_message(subscriber._client, None, msg)
            loop.run_until_complete(asyncio.sleep(0.05))
        finally:
            loop.close()

        assert mock_insert.call_count == 1

    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_downsample_stores_after_60s(
        self, mock_insert: AsyncMock, sample_tasmota_payload: bytes
    ) -> None:
        subscriber = self._make_subscriber("downsample_60s")
        loop = asyncio.new_event_loop()
        subscriber._loop = loop

        msg = self._make_message("tele/dev1/SENSOR", sample_tasmota_payload)

        try:
            # First message: stored
            subscriber._on_message(subscriber._client, None, msg)
            # Simulate 61s passing
            subscriber._last_stored["dev1"] = time.monotonic() - 61.0
            # Second message: stored (enough time passed)
            subscriber._on_message(subscriber._client, None, msg)
            loop.run_until_complete(asyncio.sleep(0.05))
        finally:
            loop.close()

        assert mock_insert.call_count == 2

    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_downsample_per_device(
        self, mock_insert: AsyncMock, sample_tasmota_payload: bytes
    ) -> None:
        subscriber = self._make_subscriber("downsample_60s")
        loop = asyncio.new_event_loop()
        subscriber._loop = loop

        msg1 = self._make_message("tele/dev1/SENSOR", sample_tasmota_payload)
        msg2 = self._make_message("tele/dev2/SENSOR", sample_tasmota_payload)

        try:
            # dev1 first message
            subscriber._on_message(subscriber._client, None, msg1)
            # dev2 first message — different device, should store
            subscriber._on_message(subscriber._client, None, msg2)
            loop.run_until_complete(asyncio.sleep(0.05))
        finally:
            loop.close()

        assert mock_insert.call_count == 2

    def test_unparseable_message_skipped(self) -> None:
        subscriber = self._make_subscriber("all")
        subscriber._loop = asyncio.new_event_loop()

        msg = self._make_message("tele/dev1/SENSOR", b"not json")
        # Should not raise
        subscriber._on_message(subscriber._client, None, msg)
        subscriber._loop.close()


class TestMqttSubscriberOnConnect:
    def test_on_connect_subscribes(self) -> None:
        settings = Settings(
            db_path=":memory:",
            mqtt_host="localhost",
            mqtt_topic="tele/+/SENSOR",
        )
        with patch("powerreader.mqtt.paho_mqtt.Client") as mock_cls:
            mock_client = mock_cls.return_value
            subscriber = MqttSubscriber(settings)
            # Simulate on_connect callback
            flags = MagicMock()
            rc = MagicMock()
            subscriber._on_connect(mock_client, None, flags, rc)
            mock_client.subscribe.assert_called_once_with("tele/+/SENSOR")
