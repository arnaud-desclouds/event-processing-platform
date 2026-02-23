from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

from .settings import Settings


def build_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.topic,
        bootstrap_servers=settings.brokers,
        group_id=settings.consumer_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


def build_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(bootstrap_servers=settings.brokers)


async def commit_message(consumer: AIOKafkaConsumer, msg: Any) -> None:
    tp = TopicPartition(msg.topic, msg.partition)
    await consumer.commit({tp: msg.offset + 1})


def extract_trace_carrier(headers: Optional[list[Tuple[str, bytes]]]) -> Dict[str, str]:
    carrier: Dict[str, str] = {}
    if not headers:
        return carrier
    for k, v in headers:
        if v is None:
            continue
        try:
            carrier[k] = v.decode("utf-8")
        except UnicodeDecodeError:
            continue
    return carrier
