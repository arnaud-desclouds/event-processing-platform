import json
import os

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

producer: AIOKafkaProducer | None = None
BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("EVENTS_TOPIC", "events.raw")


class Event(BaseModel):
    event_id: str
    timestamp: str
    source: str
    type: str
    payload: dict


@app.on_event("startup")
async def startup():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=BROKERS)
    await producer.start()


@app.on_event("shutdown")
async def shutdown():
    await producer.stop()


@app.post("/v1/events")
async def ingest_event(event: Event):
    await producer.send_and_wait(TOPIC, json.dumps(event.dict()).encode())
    return {"status": "accepted"}


@app.get("/health")
def health():
    return {"status": "ok"}
