"""Tests for the MQTT subscriber and message parsing."""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

from powerreader.config import Settings
from powerreader.mqtt import (
    MqttSubscriber,
    _resolve_dotted,
    extract_device_id,
    parse_field_map,
    parse_tasmota_message,
)


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
        assert parse_tasmota_message(b'{"LK13BE": {"total": 1}}') is None

    def test_non_dict_payload(self) -> None:
        assert parse_tasmota_message(b"[1, 2, 3]") is None

    def test_custom_field_map(self) -> None:
        """Test parsing with a non-default (SML) field mapping."""
        import json

        payload = json.dumps(
            {
                "Time": "2024-01-15T14:30:00",
                "SML": {"Total_in": 42000.5, "Power_curr": 538},
            }
        ).encode()
        custom = {"total_in": "SML.Total_in", "power_w": "SML.Power_curr"}
        result = parse_tasmota_message(payload, field_map=custom)
        assert result is not None
        assert result["total_in"] == 42000.5
        assert result["power_w"] == 538.0
        assert "voltage" not in result


class TestParseFieldMap:
    def test_empty_string_returns_defaults(self) -> None:
        from powerreader.mqtt import DEFAULT_FIELD_MAP

        assert parse_field_map("") is DEFAULT_FIELD_MAP
        assert parse_field_map("  ") is DEFAULT_FIELD_MAP

    def test_parses_comma_separated_pairs(self) -> None:
        result = parse_field_map("total_in=SML.Total_in,power_w=SML.Power_curr")
        assert result == {
            "total_in": "SML.Total_in",
            "power_w": "SML.Power_curr",
        }

    def test_strips_whitespace(self) -> None:
        result = parse_field_map(" total_in = SML.Total_in , power_w = SML.Power_curr ")
        assert result == {
            "total_in": "SML.Total_in",
            "power_w": "SML.Power_curr",
        }

    def test_skips_invalid_entries(self) -> None:
        raw = "total_in=SML.Total_in,bad_entry,power_w=SML.Power_curr"
        result = parse_field_map(raw)
        assert len(result) == 2
        assert "total_in" in result
        assert "power_w" in result


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

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_store_all_mode(
        self, mock_insert: AsyncMock, mock_log: AsyncMock, sample_tasmota_payload: bytes
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

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_downsample_skips_within_60s(
        self, mock_insert: AsyncMock, mock_log: AsyncMock, sample_tasmota_payload: bytes
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

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_downsample_stores_after_60s(
        self, mock_insert: AsyncMock, mock_log: AsyncMock, sample_tasmota_payload: bytes
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

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_downsample_per_device(
        self, mock_insert: AsyncMock, mock_log: AsyncMock, sample_tasmota_payload: bytes
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

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    def test_unparseable_message_skipped(self, mock_log: AsyncMock) -> None:
        subscriber = self._make_subscriber("all")
        loop = asyncio.new_event_loop()
        subscriber._loop = loop

        msg = self._make_message("tele/dev1/SENSOR", b"not json")
        try:
            subscriber._on_message(subscriber._client, None, msg)
            loop.run_until_complete(asyncio.sleep(0.05))
        finally:
            loop.close()

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_log_ok_on_valid_message(
        self, mock_insert: AsyncMock, mock_log: AsyncMock, sample_tasmota_payload: bytes
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

        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][2] == "ok"
        assert "538.0W" in call_args[0][3]
        assert "42000.5kWh" in call_args[0][3]

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    def test_log_invalid_on_unparseable(self, mock_log: AsyncMock) -> None:
        subscriber = self._make_subscriber("all")
        loop = asyncio.new_event_loop()
        subscriber._loop = loop

        msg = self._make_message("tele/dev1/SENSOR", b"not json")
        try:
            subscriber._on_message(subscriber._client, None, msg)
            loop.run_until_complete(asyncio.sleep(0.05))
        finally:
            loop.close()

        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][1] == "dev1"
        assert call_args[0][2] == "invalid"
        assert call_args[0][3] == "unparseable payload"

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    @patch(
        "powerreader.mqtt.parse_tasmota_message",
        side_effect=RuntimeError("boom"),
    )
    def test_log_error_on_exception(
        self, mock_parse: MagicMock, mock_log: AsyncMock
    ) -> None:
        subscriber = self._make_subscriber("all")
        loop = asyncio.new_event_loop()
        subscriber._loop = loop

        msg = self._make_message("tele/dev1/SENSOR", b'{"Time":"now"}')
        try:
            subscriber._on_message(subscriber._client, None, msg)
            loop.run_until_complete(asyncio.sleep(0.05))
        finally:
            loop.close()

        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][2] == "error"
        assert "boom" in call_args[0][3]


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


class TestDefectPayloads:
    """Tests for edge-case and defective MQTT payloads."""

    def _payload(self, lk13be_fields: dict, time: str = "2024-01-15T14:30:00") -> bytes:
        return json.dumps({"Time": time, "LK13BE": lk13be_fields}).encode()

    def test_nan_value_treated_as_none(self) -> None:
        result = parse_tasmota_message(self._payload({"total": "NaN"}))
        assert result is not None
        assert result["total_in"] is None

    def test_nan_float_treated_as_none(self) -> None:
        assert _resolve_dotted({"x": float("nan")}, "x") is None

    def test_infinity_treated_as_none(self) -> None:
        result = parse_tasmota_message(self._payload({"total": "Infinity"}))
        assert result is not None
        assert result["total_in"] is None

    def test_negative_infinity_treated_as_none(self) -> None:
        assert _resolve_dotted({"x": float("-inf")}, "x") is None

    def test_extremely_large_number_passes(self) -> None:
        """Large but finite numbers are accepted (no range validation)."""
        result = parse_tasmota_message(self._payload({"total": 1e20}))
        assert result is not None
        assert result["total_in"] == 1e20

    def test_negative_power_passes(self) -> None:
        """Negative values are accepted (no range validation)."""
        result = parse_tasmota_message(self._payload({"current": -500}))
        assert result is not None
        assert result["power_w"] == -500.0

    def test_string_in_numeric_field(self) -> None:
        result = parse_tasmota_message(self._payload({"total": "abc"}))
        assert result is not None
        assert result["total_in"] is None

    def test_empty_nested_object(self) -> None:
        result = parse_tasmota_message(self._payload({}))
        assert result is not None
        assert result["total_in"] is None
        assert result["power_w"] is None

    def test_wrong_nesting_string(self) -> None:
        payload = json.dumps(
            {"Time": "2024-01-15T14:30:00", "LK13BE": "not_a_dict"}
        ).encode()
        result = parse_tasmota_message(payload)
        assert result is not None
        assert result["total_in"] is None

    def test_extra_unexpected_fields(self) -> None:
        result = parse_tasmota_message(
            self._payload({"total": 42000.5, "unknown_field": 999, "bonus": "data"})
        )
        assert result is not None
        assert result["total_in"] == 42000.5

    def test_empty_time_field(self) -> None:
        result = parse_tasmota_message(self._payload({"total": 100}, time=""))
        assert result is not None
        assert result["timestamp"] == ""

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    def test_on_message_survives_insert_exception(self, mock_log: AsyncMock) -> None:
        """_on_message should not raise even if insert_reading throws."""
        settings = Settings(db_path=":memory:", mqtt_host="localhost")
        with patch("powerreader.mqtt.paho_mqtt.Client"):
            subscriber = MqttSubscriber(settings)
        loop = asyncio.new_event_loop()
        subscriber._loop = loop

        msg = MagicMock()
        msg.topic = "tele/dev1/SENSOR"
        msg.payload = self._payload({"total": 42000.5})

        with patch(
            "powerreader.mqtt.insert_reading",
            side_effect=RuntimeError("DB exploded"),
        ):
            # Should not raise
            subscriber._on_message(subscriber._client, None, msg)

        loop.close()

    @patch("powerreader.mqtt.insert_mqtt_log", new_callable=AsyncMock)
    @patch("powerreader.mqtt.insert_reading", new_callable=AsyncMock)
    def test_on_message_survives_unexpected_payload(
        self, mock_insert: AsyncMock, mock_log: AsyncMock
    ) -> None:
        """_on_message should not raise on completely unexpected structures."""
        settings = Settings(db_path=":memory:", mqtt_host="localhost")
        with patch("powerreader.mqtt.paho_mqtt.Client"):
            subscriber = MqttSubscriber(settings)
        subscriber._loop = asyncio.new_event_loop()

        msg = MagicMock()
        msg.topic = "tele/dev1/SENSOR"
        msg.payload = b'{"Time": "now", "LK13BE": [1,2,3]}'

        # Should not raise
        subscriber._on_message(subscriber._client, None, msg)
        subscriber._loop.close()
