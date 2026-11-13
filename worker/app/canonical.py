"""Value conversion and record keys. Pure functions, no I/O.

Input values come from json.loads(..., parse_float=Decimal), so numbers keep their exact text.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

METADATA_PREFIX = "_airbyte_"


def _types(prop: dict[str, Any]) -> list[str]:
    t = prop.get("type")
    return [t] if isinstance(t, str) else list(t or [])


def _is_integer(prop: dict[str, Any]) -> bool:
    """source-postgres declares integers as {"type": "number", "airbyte_type": "integer"}."""
    types = _types(prop)
    return prop.get("airbyte_type") == "integer" or ("integer" in types and "number" not in types)


def _is_tz_timestamp(prop: dict[str, Any]) -> bool:
    if prop.get("airbyte_type") == "timestamp_with_timezone":
        return True
    return prop.get("format") == "date-time" and prop.get("airbyte_type") != "timestamp_without_timezone"


def normalize_timestamp(value: str) -> str:
    """ISO 8601 with offset ('Z' becomes '+00:00'). A value without an offset is returned unchanged."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    return value if parsed.tzinfo is None else parsed.isoformat()


def convert_value(value: Any, prop: dict[str, Any] | None = None) -> Any:
    """Convert one value: decimals to strings (exact scale), timestamps with offset, null kept."""
    prop = prop or {}
    types = _types(prop)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        if _is_integer(prop) and value == value.to_integral_value():
            return int(value)
        return format(value, "f")
    if isinstance(value, int):
        return str(value) if "number" in types and not _is_integer(prop) else value
    if isinstance(value, str):
        return normalize_timestamp(value) if _is_tz_timestamp(prop) else value
    if isinstance(value, dict):
        props = prop.get("properties", {})
        return {k: convert_value(v, props.get(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [convert_value(v, prop.get("items")) for v in value]
    return value


def convert_record(data: dict[str, Any], schema: dict[str, Any] | None) -> dict[str, Any]:
    """Drop _airbyte_* metadata, convert values. Absent fields are not added; explicit nulls stay."""
    props = (schema or {}).get("properties", {})
    return {k: convert_value(v, props.get(k)) for k, v in data.items() if not k.startswith(METADATA_PREFIX)}


def extract_key(data: dict[str, Any], key_fields: list[str] | None) -> tuple[list[str] | None, str | None]:
    """Return (key, None) or (None, reason). A key is never invented."""
    if not key_fields:
        return None, "no_key_defined"
    key: list[str] = []
    for field in key_fields:
        if field not in data:
            return None, f"missing_key_field:{field}"
        value = data[field]
        if value is None:
            return None, f"null_key_field:{field}"
        if isinstance(value, bool) or not isinstance(value, (str, int)) or str(value).strip() == "":
            return None, f"invalid_key_field:{field}"
        key.append(str(value))
    return key, None


def to_output(stream: str, data: dict[str, Any], schema: dict[str, Any] | None,
              key_fields: list[str] | None) -> dict[str, Any]:
    """The record handed to IRIS: {"stream", "key", "data"} plus "key_error" when the key is flagged."""
    converted = convert_record(data, schema)
    key, error = extract_key(converted, key_fields)
    out: dict[str, Any] = {"stream": stream, "key": key, "data": converted}
    if error:
        out["key_error"] = error
    return out
