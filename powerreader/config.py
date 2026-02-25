from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    ``poll_store_mode`` controls message storage:
      - ``"all"``            — store every incoming MQTT message
      - ``"downsample_60s"`` — store at most one message per device per minute

    ``field_map`` overrides the default Tasmota field mapping.
    Format: ``"total_in=SML.Total_in,power_w=SML.Power_curr"``
    Leave empty to use the built-in LK13BE defaults (see mqtt.DEFAULT_FIELD_MAP).
    """

    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_pass: str = ""
    mqtt_tls: bool = False
    mqtt_tls_ca: str = ""
    mqtt_topic: str = "tele/+/SENSOR"
    db_path: str = "/data/powerreader.db"
    poll_store_mode: str = "all"
    raw_retention_days: int = 30
    web_port: int = 8080
    field_map: str = ""
    allowed_devices: str = ""
