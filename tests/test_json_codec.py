from __future__ import annotations

from datetime import UTC, datetime

from services.processor_worker.json_codec import dumps_json_bytes, loads_json_bytes


def test_json_codec_round_trips_datetime_payloads() -> None:
    payload = {
        "event_id": "evt-1",
        "timestamp": datetime(2026, 1, 1, tzinfo=UTC),
        "payload": {"status": "ok"},
    }

    encoded = dumps_json_bytes(payload)
    decoded = loads_json_bytes(encoded)

    assert decoded == {
        "event_id": "evt-1",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "payload": {"status": "ok"},
    }
