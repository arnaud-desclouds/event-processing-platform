from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator

NonEmptyText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class EventModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: NonEmptyText
    timestamp: datetime
    type: NonEmptyText
    source: NonEmptyText
    payload: Any

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include timezone information")
        return value

    @field_validator("payload")
    @classmethod
    def payload_must_not_be_null(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("payload must not be null")
        return value


def validate_event(obj: Any) -> dict[str, Any]:
    """Return a validated event as a plain dict."""
    return EventModel.model_validate(obj).model_dump()
