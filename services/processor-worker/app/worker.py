import asyncio
import json
import os

import asyncpg
from aiokafka import AIOKafkaConsumer

BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("EVENTS_TOPIC", "events.raw")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "events")
POSTGRES_USER = os.getenv("POSTGRES_USER", "events_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "events_password")


async def main():
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
            event = json.loads(msg.value.decode())

            await conn.execute(
                "INSERT INTO events(event_id, type, source, payload) VALUES($1,$2,$3,$4)",
                event["event_id"],
                event["type"],
                event["source"],
                json.dumps(event["payload"]),
            )

            print(
                f"processed event_id={event['event_id']} "
                f"type={event['type']} "
                f"source={event['source']}"
            )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
