from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from structlog.stdlib import BoundLogger

from .json_codec import dumps_json_bytes, loads_json_bytes
from .observability import configure_logging, get_logger
from .settings import Settings

JSONDict = dict[str, Any]


@dataclass(slots=True)
class ReplayCandidate:
    failed_event: JSONDict
    event_id: str | None
    reason: str | None


@dataclass(slots=True)
class ReplayStats:
    processed: int = 0
    matched: int = 0
    published: int = 0
    dry_run: int = 0
    skipped: int = 0


def _extract_failed_event(dlq_msg: Mapping[str, Any]) -> tuple[JSONDict | None, str | None]:
    """Handle DLQ entries with either failed_event or raw-only payloads."""
    failed_event = dlq_msg.get("failed_event")
    if isinstance(failed_event, dict):
        return failed_event, dlq_msg.get("reason") if isinstance(
            dlq_msg.get("reason"), str
        ) else None
    return None, dlq_msg.get("reason") if isinstance(dlq_msg.get("reason"), str) else None


def _build_clients(
    *,
    args: argparse.Namespace,
) -> tuple[AIOKafkaConsumer, AIOKafkaProducer]:
    consumer = AIOKafkaConsumer(
        args.dlq_topic,
        bootstrap_servers=args.brokers,
        group_id=args.group_id,
        enable_auto_commit=args.commit_offsets,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=args.brokers)
    return consumer, producer


def _log_replay_started(*, args: argparse.Namespace, log: BoundLogger) -> None:
    log.info(
        "dlq_replay_started",
        brokers=args.brokers,
        dlq_topic=args.dlq_topic,
        target_topic=args.target_topic,
        group_id=args.group_id,
        from_beginning=args.from_beginning,
        commit_offsets=args.commit_offsets,
        dry_run=args.dry_run,
        reason_filter=args.reason or None,
        event_id_filter=args.event_id or None,
        max_messages=args.max_messages or None,
    )


def _build_replay_candidate(
    *,
    raw: bytes | None,
    args: argparse.Namespace,
) -> ReplayCandidate | None:
    if raw is None:
        return None

    try:
        dlq_payload = loads_json_bytes(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if not isinstance(dlq_payload, dict):
        return None

    failed_event, reason = _extract_failed_event(dlq_payload)
    if failed_event is None:
        return None
    if args.reason and reason != args.reason:
        return None

    event_id_value = failed_event.get("event_id")
    event_id = event_id_value if isinstance(event_id_value, str) else None
    if args.event_id and event_id != args.event_id:
        return None

    return ReplayCandidate(
        failed_event=failed_event,
        event_id=event_id,
        reason=reason,
    )


async def _replay_candidate(
    *,
    candidate: ReplayCandidate,
    producer: AIOKafkaProducer,
    target_topic: str,
    log: BoundLogger,
    dry_run: bool,
) -> bool:
    if dry_run:
        log.info(
            "dlq_replay_dry_run",
            event_id=candidate.event_id,
            reason=candidate.reason,
        )
        return False

    try:
        await producer.send_and_wait(
            target_topic,
            dumps_json_bytes(candidate.failed_event),
        )
    except KafkaError as exc:
        log.error(
            "dlq_replay_publish_failed",
            event_id=candidate.event_id,
            reason=candidate.reason,
            err=type(exc).__name__,
        )
        raise

    log.info(
        "dlq_replay_published",
        event_id=candidate.event_id,
        reason=candidate.reason,
    )
    return True


async def _stop_clients(
    *,
    consumer: AIOKafkaConsumer,
    producer: AIOKafkaProducer,
) -> None:
    with suppress(Exception):
        await consumer.stop()
    with suppress(Exception):
        await producer.stop()


def _log_replay_finished(
    *,
    args: argparse.Namespace,
    stats: ReplayStats,
    log: BoundLogger,
) -> None:
    log.info(
        "dlq_replay_finished",
        processed=stats.processed,
        matched=stats.matched,
        published=stats.published,
        dry_run=stats.dry_run,
        skipped=stats.skipped,
        dlq_topic=args.dlq_topic,
        target_topic=args.target_topic,
        commit_offsets=args.commit_offsets,
    )


async def replay(args: argparse.Namespace) -> int:
    settings = Settings()
    configure_logging(settings.log_level)
    log = get_logger("dlq-replay")

    consumer, producer = _build_clients(args=args)

    await consumer.start()
    await producer.start()

    stats = ReplayStats()
    _log_replay_started(args=args, log=log)

    try:
        async for msg in consumer:
            stats.processed += 1
            candidate = _build_replay_candidate(raw=msg.value, args=args)
            if candidate is None:
                stats.skipped += 1
            else:
                stats.matched += 1
                if await _replay_candidate(
                    candidate=candidate,
                    producer=producer,
                    target_topic=args.target_topic,
                    log=log,
                    dry_run=args.dry_run,
                ):
                    stats.published += 1
                else:
                    stats.dry_run += 1

            if args.max_messages and stats.matched >= args.max_messages:
                break

    finally:
        await _stop_clients(consumer=consumer, producer=producer)
        _log_replay_finished(args=args, stats=stats, log=log)

    return 0


def build_parser() -> argparse.ArgumentParser:
    settings = Settings()

    p = argparse.ArgumentParser(description="Replay events from DLQ to a target topic.")
    p.add_argument("--brokers", default=settings.brokers)
    p.add_argument("--dlq-topic", default=settings.dlq_topic)
    p.add_argument(
        "--target-topic",
        default=settings.topic,
        help="Target topic for replayed events.",
    )

    p.add_argument("--group-id", default="dlq-replay")
    p.add_argument(
        "--from-beginning", action="store_true", help="Consume DLQ from earliest offset."
    )
    p.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N matching DLQ messages (0 = unlimited).",
    )

    p.add_argument("--reason", default="", help="Only replay DLQ entries matching this reason.")
    p.add_argument("--event-id", default="", help="Only replay a specific event_id.")

    p.add_argument(
        "--dry-run", action="store_true", help="Do not publish, only log what would be replayed."
    )
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
