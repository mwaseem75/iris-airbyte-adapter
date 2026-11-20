"""Connector specs: fetch (cached per image), find secret fields, validate a config against the spec.

Validation problems carry only a path and a short reason, never the value: configs hold credentials.
"""
from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path
from typing import Any, Callable

from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError, best_match
from jsonschema.validators import validator_for

SPECS_DIR = Path(os.getenv("SPECS_DIR", "/data/specs"))
_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_cache: dict[tuple[str, str], dict[str, Any]] = {}
_lock = threading.Lock()


def secret_fields(schema: dict[str, Any], prefix: str = "") -> list[str]:
    """Dotted paths of every property marked airbyte_secret, including those inside oneOf/anyOf/allOf
    options (an option's properties sit at the same level as the parent)."""
    found: set[str] = set()

    def walk(node: Any, path: str) -> None:
        if not isinstance(node, dict):
            return
        for name, sub in (node.get("properties") or {}).items():
            p = f"{path}.{name}" if path else name
            if isinstance(sub, dict) and sub.get("airbyte_secret") is True:
                found.add(p)
            walk(sub, p)
        for key in ("oneOf", "anyOf", "allOf"):
            for option in node.get(key) or []:
                walk(option, path)
        walk(node.get("items"), path)

    walk(schema, prefix)
    return sorted(found)


def _reason(err: ValidationError) -> str:
    v = err.validator
    if v == "type":
        t = err.validator_value
        return "must be of type " + (" or ".join(t) if isinstance(t, list) else str(t))
    if v == "enum":
        return "is not one of the allowed values"
    if v == "const":
        return "has a fixed value that does not match"
    if v in ("oneOf", "anyOf"):
        return "does not match any allowed option"
    if v in ("pattern", "format"):
        return "has an invalid format"
    if v in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "minLength", "maxLength"):
        return "is out of the allowed range"
    return f"fails the '{v}' check"


def problems(config: dict[str, Any], schema: dict[str, Any]) -> list[dict[str, str]]:
    """[{path, reason}] for everything wrong with config; empty if valid. No values in the output."""
    validator = validator_for(schema, Draft7Validator)(schema)
    out: list[dict[str, str]] = []
    for err in validator.iter_errors(config):
        while err.validator in ("oneOf", "anyOf") and err.context:
            deeper = best_match(err.context)
            if deeper is None:
                break
            err = deeper
        path = [str(p) for p in err.absolute_path]
        if err.validator == "required":
            path += [n for n in err.validator_value if isinstance(err.instance, dict) and n not in err.instance][:1]
            reason = "is required"
        else:
            reason = _reason(err)
        out.append({"path": ".".join(path), "reason": reason})
    seen: set[tuple[str, str]] = set()
    return [p for p in sorted(out, key=lambda p: (p["path"], p["reason"]))
            if (p["path"], p["reason"]) not in seen and not seen.add((p["path"], p["reason"]))]


def get_spec(connector: str, version: str, fetch: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Spec for an image: memory, then /data/specs, then fetch() (runs the connector's spec command)."""
    if not (_SAFE.match(connector) and _SAFE.match(version)):
        raise ValueError("invalid connector or version")
    key = (connector, version)
    with _lock:
        if key in _cache:
            return _cache[key]
    path = SPECS_DIR / f"{connector}-{version}.json"
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        schema = fetch()["connection_specification"]
        try:
            SPECS_DIR.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(schema), encoding="utf-8")
        except OSError:
            pass  # cache is an optimisation only
    spec = {"connector": connector, "version": version, "connection_specification": schema,
            "secret_fields": secret_fields(schema)}
    with _lock:
        _cache[key] = spec
    return spec
