from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_pass: str = ""
    mqtt_topic: str = "tele/+/SENSOR"
    db_path: str = "/data/powerreader.db"
    poll_store_mode: str = "all"
    raw_retention_days: int = 30
    web_port: int = 8080
