from __future__ import annotations

import json
from datetime import datetime
from typing import Any

JSONDict = dict[str, Any]


def loads_json_bytes(b: bytes) -> JSONDict:
    return json.loads(b.decode("utf-8"))


def dumps_json_bytes(obj: JSONDict) -> bytes:
    return json.dumps(
        obj,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
