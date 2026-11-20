"""Cache of a connector's discovered catalog (the list of streams and their schemas).

Discovering starts the connector container, which takes seconds, and a read would do it again every run.
The cache key is the connector, its version and a fingerprint of the config. The fingerprint is a salted
hash: the config itself (which holds credentials) is never written to disk. An entry older than the TTL
(CATALOG_CACHE_TTL_SECONDS, 0 turns the cache off) is ignored and replaced.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

CACHE_DIR = Path(os.getenv("CATALOG_DIR", "/data/catalogs"))


def ttl_seconds() -> int:
    return int(os.getenv("CATALOG_CACHE_TTL_SECONDS", "3600"))


def _salt() -> bytes:
    """A random value kept next to the cache, so the fingerprint cannot be guessed from a known config."""
    path = CACHE_DIR / ".salt"
    try:
        return path.read_bytes()
    except FileNotFoundError:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        value = os.urandom(16).hex().encode()
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(value)
        os.replace(tmp, path)
        return value


def fingerprint(connector: str, version: str, config: dict[str, Any]) -> str:
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(_salt() + canonical.encode("utf-8")).hexdigest()[:32]
    return f"{connector}-{version}-{digest}"


def load(key: str) -> dict[str, Any] | None:
    if ttl_seconds() <= 0:
        return None
    path = CACHE_DIR / f"{key}.json"
    try:
        if time.time() - path.stat().st_mtime > ttl_seconds():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError, OSError):
        return None


def save(key: str, catalog: dict[str, Any]) -> None:
    if ttl_seconds() <= 0:
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{key}.json"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(catalog), encoding="utf-8")
    os.replace(tmp, path)
