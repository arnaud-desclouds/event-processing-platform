from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None,
        extra="ignore",
        frozen=True,
    )

    brokers: str = Field(default="redpanda:9092", validation_alias="REDPANDA_BROKERS")
    topic: str = Field(default="events.raw", validation_alias="EVENTS_TOPIC")
    dlq_topic: str = Field(default="events.dlq", validation_alias="DLQ_TOPIC")
    consumer_group_id: str = Field(
        default="processor-group", validation_alias="WORKER_CONSUMER_GROUP"
    )

    postgres_host: str = Field(default="postgres", validation_alias="POSTGRES_HOST")
    postgres_db: str = Field(default="events", validation_alias="POSTGRES_DB")
    postgres_user: str = Field(default="events_user", validation_alias="POSTGRES_USER")
    postgres_password: str = Field(default="events_password", validation_alias="POSTGRES_PASSWORD")
    postgres_port: int = Field(default=5432, validation_alias="POSTGRES_PORT")

    otel_exporter_otlp_endpoint: str = Field(
        default="http://otel-collector:4317",
        validation_alias="OTEL_EXPORTER_OTLP_ENDPOINT",
    )
    service_name: str = Field(default="processor-worker", validation_alias="OTEL_SERVICE_NAME")
    metrics_port: int = Field(default=8000, validation_alias="WORKER_METRICS_PORT")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    health_host: str = Field(default="0.0.0.0", validation_alias="WORKER_HEALTH_HOST")
    health_port: int = Field(default=8081, validation_alias="WORKER_HEALTH_PORT")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:"
            f"{self.postgres_password}@"
            f"{self.postgres_host}:"
            f"{self.postgres_port}/"
            f"{self.postgres_db}"
        )
