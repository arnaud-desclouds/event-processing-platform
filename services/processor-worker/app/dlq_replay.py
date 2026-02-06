import argparse
import asyncio
import json
import os
from typing import Any, Dict, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


def _loads(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))


def _dumps(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _extract_failed_event(dlq_msg: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Supports two DLQ formats:
    1) {"failed_event": {...}, "failed_at": ..., "reason": "..."}
    2) {"raw": "...", "reason": "..."}  (invalid_json case)
    """
    if isinstance(dlq_msg, dict) and "failed_event" in dlq_msg and isinstance(dlq_msg["failed_event"], dict):
        return dlq_msg["failed_event"], dlq_msg.get("reason")
    return None, dlq_msg.get("reason") if isinstance(dlq_msg, dict) else None


async def replay(args: argparse.Namespace) -> int:
    brokers = args.brokers
    dlq_topic = args.dlq_topic
    out_topic = args.out_topic

    consumer = AIOKafkaConsumer(
        dlq_topic,
        bootstrap_servers=brokers,
        group_id=args.group_id,
        enable_auto_commit=args.commit_offsets,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=brokers)

    await consumer.start()
    await producer.start()

    processed = 0
    replayed = 0
    skipped = 0

    try:
        async for msg in consumer:
            processed += 1
            if args.max_messages and processed > args.max_messages:
                break

            try:
                dlq_payload = _loads(msg.value)
            except Exception:
                # If DLQ message isn't JSON, we can't safely replay it.
                skipped += 1
                continue

            failed_event, reason = _extract_failed_event(dlq_payload)

            # Optional filters
            if args.reason and reason != args.reason:
                skipped += 1
                continue

            if failed_event is None:
                # invalid_json case: nothing safe to replay into events.raw
                skipped += 1
                continue

            event_id = failed_event.get("event_id")
            if args.event_id and event_id != args.event_id:
                skipped += 1
                continue

            if args.dry_run:
                replayed += 1
                print(f"DRY_RUN replay event_id={event_id} reason={reason}")
            else:
                await producer.send_and_wait(out_topic, _dumps(failed_event))
                replayed += 1
                print(f"replayed event_id={event_id} reason={reason}")

    finally:
        await consumer.stop()
        await producer.stop()

    print(
        f"done processed={processed} replayed={replayed} skipped={skipped} "
        f"dlq_topic={dlq_topic} out_topic={out_topic} commit_offsets={args.commit_offsets}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Replay events from DLQ back into the main topic.")
    p.add_argument("--brokers", default=os.getenv("REDPANDA_BROKERS", "redpanda:9092"))
    p.add_argument("--dlq-topic", default=os.getenv("DLQ_TOPIC", "events.dlq"))
    p.add_argument("--out-topic", default=os.getenv("EVENTS_TOPIC", "events.raw"))

    p.add_argument("--group-id", default="dlq-replay")
    p.add_argument("--from-beginning", action="store_true", help="Consume DLQ from earliest offset.")
    p.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 = unlimited).")

    p.add_argument("--reason", default="", help="Only replay DLQ entries matching this reason.")
    p.add_argument("--event-id", default="", help="Only replay a specific event_id.")

    p.add_argument("--dry-run", action="store_true", help="Do not publish, only print what would be replayed.")
    p.add_argument(
        "--commit-offsets",
        action="store_true",
        help="Commit consumer offsets while replaying (useful to make replay idempotent).",
    )
    return p


def main() -> int:
    args = build_parser().parse_args()
    return asyncio.run(replay(args))


if __name__ == "__main__":
    raise SystemExit(main())
