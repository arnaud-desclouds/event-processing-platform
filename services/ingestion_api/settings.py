from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    brokers: str = Field(default="redpanda:9092", validation_alias="REDPANDA_BROKERS")
    topic: str = Field(default="events.raw", validation_alias="EVENTS_TOPIC")
    publisher_ready_timeout_seconds: float = Field(
        default=1.0,
        validation_alias="PUBLISHER_READY_TIMEOUT_SECONDS",
    )
    publisher_start_max_attempts: int = Field(
        default=5,
        ge=1,
        validation_alias="PUBLISHER_START_MAX_ATTEMPTS",
    )
    publisher_start_backoff_seconds: float = Field(
        default=2.0,
        ge=0.0,
        validation_alias="PUBLISHER_START_BACKOFF_SECONDS",
    )

    otel_exporter_otlp_endpoint: str = Field(
        default="http://otel-collector:4317",
        validation_alias="OTEL_EXPORTER_OTLP_ENDPOINT",
    )
    service_name: str = Field(default="ingestion-api", validation_alias="OTEL_SERVICE_NAME")
