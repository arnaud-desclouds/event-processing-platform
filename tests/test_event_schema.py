from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from services.common import validate_event


def test_validate_event_accepts_shared_contract() -> None:
    event = validate_event(
        {
            "event_id": "evt-1",
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "signup",
            "source": "web",
            "payload": {"user_id": "u-1"},
        }
    )

    assert event["event_id"] == "evt-1"
    assert event["timestamp"] == datetime.fromisoformat("2026-01-01T00:00:00+00:00")


def test_validate_event_strips_whitespace_from_required_text_fields() -> None:
    event = validate_event(
        {
            "event_id": "  evt-1  ",
            "timestamp": "2026-01-01T00:00:00Z",
            "type": " signup ",
            "source": " web ",
            "payload": {"user_id": "u-1"},
        }
    )

    assert event["event_id"] == "evt-1"
    assert event["type"] == "signup"
    assert event["source"] == "web"


def test_validate_event_rejects_missing_timestamp() -> None:
    with pytest.raises(ValidationError):
        validate_event(
            {
                "event_id": "evt-1",
                "type": "signup",
                "source": "web",
                "payload": {"user_id": "u-1"},
            }
        )


def test_validate_event_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError):
        validate_event(
            {
                "event_id": "evt-1",
                "timestamp": "2026-01-01T00:00:00",
                "type": "signup",
                "source": "web",
                "payload": {"user_id": "u-1"},
            }
        )


def test_validate_event_preserves_timezone_aware_timestamp() -> None:
    event = validate_event(
        {
            "event_id": "evt-1",
            "timestamp": "2026-01-01T01:00:00+01:00",
            "type": "signup",
            "source": "web",
            "payload": {"user_id": "u-1"},
        }
    )

    assert event["timestamp"] == datetime.fromisoformat("2026-01-01T01:00:00+01:00")


def test_validate_event_rejects_null_payload() -> None:
    with pytest.raises(ValidationError):
        validate_event(
            {
                "event_id": "evt-1",
                "timestamp": "2026-01-01T00:00:00Z",
                "type": "signup",
                "source": "web",
                "payload": None,
            }
        )


@pytest.mark.parametrize("field_name", ["event_id", "type", "source"])
def test_validate_event_rejects_blank_required_text_fields(field_name: str) -> None:
    payload = {
        "event_id": "evt-1",
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "signup",
        "source": "web",
        "payload": {"user_id": "u-1"},
    }
    payload[field_name] = "   "

    with pytest.raises(ValidationError):
        validate_event(payload)
