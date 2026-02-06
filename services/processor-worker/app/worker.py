import asyncio
import json
import os
import time

import asyncpg
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("EVENTS_TOPIC", "events.raw")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "events")
POSTGRES_USER = os.getenv("POSTGRES_USER", "events_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "events_password")

METRICS_PORT = int(os.getenv("WORKER_METRICS_PORT", "8000"))

EVENTS_PROCESSED_TOTAL = Counter(
    "events_processed_total",
    "Total number of events processed by the worker",
    ["event_type", "source"],
)

EVENT_PROCESSING_DURATION_SECONDS = Histogram(
    "event_processing_duration_seconds",
    "Time spent processing a single event in seconds",
)


async def main():
    start_http_server(METRICS_PORT)
    print(f"metrics server listening on :{METRICS_PORT}")

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        group_id="processor-group",
    )
    await consumer.start()

    conn = await asyncpg.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    await conn.execute("""
                       CREATE TABLE IF NOT EXISTS events (
                                                             id SERIAL PRIMARY KEY,
                                                             event_id TEXT,
                                                             type TEXT,
                                                             source TEXT,
                                                             payload JSONB,
                                                             created_at TIMESTAMP DEFAULT NOW()
                           )
                       """)

    print("worker started, waiting for events...")

    try:
        async for msg in consumer:
            start = time.perf_counter()
            event = json.loads(msg.value.decode())

            await conn.execute(
                "INSERT INTO events(event_id, type, source, payload) VALUES($1,$2,$3,$4)",
                event["event_id"],
                event["type"],
                event["source"],
                json.dumps(event["payload"]),
            )

            EVENTS_PROCESSED_TOTAL.labels(event_type=event["type"], source=event["source"]).inc()
            EVENT_PROCESSING_DURATION_SECONDS.observe(time.perf_counter() - start)

            print(
                f"processed event_id={event['event_id']} "
                f"type={event['type']} "
                f"source={event['source']}"
            )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
