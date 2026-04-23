from __future__ import annotations

import asyncio
import json
from typing import Self

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import HTTPException
from opentelemetry import propagate, trace

from services.common import EventModel

from .observability import record_ingested_event


class EventPublisher:
    def __init__(
        self,
        *,
        producer: AIOKafkaProducer | None,
        topic: str,
        readiness_timeout_seconds: float = 1.0,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._readiness_timeout_seconds = readiness_timeout_seconds

    async def is_ready(self) -> bool:
        if self._producer is None:
            return False

        try:
            partitions = await asyncio.wait_for(
                self._producer.partitions_for(self._topic),
                timeout=self._readiness_timeout_seconds,
            )
        except (asyncio.TimeoutError, KafkaError):
            return False
        except Exception:
            return False
        return partitions is not None and len(partitions) > 0

    @classmethod
    async def start(
        cls,
        *,
        brokers: str,
        topic: str,
        readiness_timeout_seconds: float = 1.0,
    ) -> Self:
        producer = AIOKafkaProducer(bootstrap_servers=brokers)
        await producer.start()
        return cls(
            producer=producer,
            topic=topic,
            readiness_timeout_seconds=readiness_timeout_seconds,
        )

    async def close(self) -> None:
        if self._producer is None:
            return

        try:
            await self._producer.stop()
        finally:
            self._producer = None

    async def publish_event(self, event: EventModel) -> None:
        if self._producer is None:
            raise HTTPException(status_code=503, detail="Kafka producer not ready")

        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("publish_event") as span:
            span.set_attribute("event.id", event.event_id)
            span.set_attribute("event.type", event.type)
            span.set_attribute("event.source", event.source)
            span.set_attribute("messaging.system", "kafka")
            span.set_attribute("messaging.destination.name", self._topic)

            carrier: dict[str, str] = {}
            propagate.inject(carrier)
            headers = [(k, v.encode("utf-8")) for k, v in carrier.items()]

            try:
                await self._producer.send_and_wait(
                    self._topic,
                    json.dumps(event.model_dump(mode="json")).encode("utf-8"),
                    headers=headers,
                )
            except KafkaError as exc:
                span.record_exception(exc)
                span.set_attribute("error", True)
                raise HTTPException(status_code=503, detail="Kafka publish failed") from exc

        record_ingested_event(event_type=event.type, source=event.source)
