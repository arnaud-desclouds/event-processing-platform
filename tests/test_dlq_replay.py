from __future__ import annotations

import argparse
from types import SimpleNamespace

import pytest

from services.processor_worker import dlq_replay


def _args(**overrides: object) -> argparse.Namespace:
    values = {
        "brokers": "broker:9092",
        "dlq_topic": "events.dlq",
        "target_topic": "events.raw",
        "group_id": "dlq-replay",
        "from_beginning": False,
        "commit_offsets": False,
        "dry_run": False,
        "reason": "",
        "event_id": "",
        "max_messages": 0,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_build_replay_candidate_returns_failed_event_when_filters_match() -> None:
    candidate = dlq_replay._build_replay_candidate(
        raw=dlq_replay.dumps_json_bytes(
            {
                "reason": "db_error",
                "failed_event": {
                    "event_id": "evt-1",
                    "type": "order.created",
                    "source": "pytest",
                    "payload": {"status": "failed"},
                },
            }
        ),
        args=_args(reason="db_error", event_id="evt-1"),
    )

    assert candidate is not None
    assert candidate.event_id == "evt-1"
    assert candidate.reason == "db_error"
    assert candidate.failed_event["type"] == "order.created"


def test_build_replay_candidate_returns_none_for_reason_only_payload() -> None:
    candidate = dlq_replay._build_replay_candidate(
        raw=dlq_replay.dumps_json_bytes(
            {
                "reason": "invalid_json",
                "raw": "{",
            }
        ),
        args=_args(),
    )

    assert candidate is None


@pytest.mark.asyncio
async def test_replay_dry_run_stops_after_matching_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    class ReplayLogCapture:
        def __init__(self) -> None:
            self.info_entries: list[tuple[str, dict[str, object]]] = []

        def info(self, event: str, **fields: object) -> None:
            self.info_entries.append((event, fields))

        def has_info(self, event: str, **fields: object) -> bool:
            return any(
                entry_event == event
                and all(entry_fields.get(key) == value for key, value in fields.items())
                for entry_event, entry_fields in self.info_entries
            )

    class InMemoryDlqMessages:
        messages: list[SimpleNamespace] = []

        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self._messages = list(type(self).messages)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        def __aiter__(self) -> "InMemoryDlqMessages":
            return self

        async def __anext__(self) -> SimpleNamespace:
            if not self._messages:
                raise StopAsyncIteration
            return self._messages.pop(0)

    class PublishedReplayMessages:
        instances: list["PublishedReplayMessages"] = []

        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self.messages: list[tuple[str, bytes]] = []
            type(self).instances.append(self)

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def send_and_wait(self, topic: str, payload: bytes) -> None:
            self.messages.append((topic, payload))

    messages = [
        SimpleNamespace(value=b"{"),
        SimpleNamespace(
            value=dlq_replay.dumps_json_bytes(
                {
                    "reason": "db_error",
                    "failed_event": {
                        "event_id": "evt-1",
                        "type": "order.created",
                        "source": "pytest",
                        "payload": {"status": "failed"},
                    },
                }
            )
        ),
        SimpleNamespace(
            value=dlq_replay.dumps_json_bytes(
                {
                    "reason": "db_error",
                    "failed_event": {
                        "event_id": "evt-2",
                        "type": "order.created",
                        "source": "pytest",
                        "payload": {"status": "failed"},
                    },
                }
            )
        ),
    ]

    log = ReplayLogCapture()
    InMemoryDlqMessages.messages = messages
    PublishedReplayMessages.instances = []
    monkeypatch.setattr(dlq_replay, "AIOKafkaConsumer", InMemoryDlqMessages)
    monkeypatch.setattr(dlq_replay, "AIOKafkaProducer", PublishedReplayMessages)
    monkeypatch.setattr(dlq_replay, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(dlq_replay, "get_logger", lambda *args, **kwargs: log)

    result = await dlq_replay.replay(_args(dry_run=True, max_messages=1))

    assert result == 0
    assert PublishedReplayMessages.instances[0].messages == []
    assert log.has_info("dlq_replay_dry_run", event_id="evt-1", reason="db_error")
    assert log.has_info(
        "dlq_replay_finished",
        processed=2,
        matched=1,
        published=0,
        dry_run=1,
        skipped=1,
        dlq_topic="events.dlq",
        target_topic="events.raw",
        commit_offsets=False,
    )
