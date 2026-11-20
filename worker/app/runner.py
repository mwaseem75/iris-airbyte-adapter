"""PyAirbyte side: check, discover, read. Runs inside the isolated child process (see isolation.py).

Reads call the connector through PyAirbyte's executor and parse its output lines directly, so that
numbers keep their exact text (no DuckDB DOUBLE, no float). State goes in and out explicitly.
Never log credentials or record payloads.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import airbyte as ab
from airbyte_protocol.models import AirbyteCatalog, SyncMode

from app import canonical, catalog_cache, staging

TEMP_DIR = Path(os.getenv("AIRBYTE_TEMP_DIR", "/tmp/airbyte/tmp"))


class JobError(Exception):
    def __init__(self, category: str, message: str) -> None:
        super().__init__(message)
        self.category = category
        self.message = message


def make_source(payload: dict[str, Any]) -> ab.Source:
    image = f"airbyte/{payload['connector']}:{payload['version']}"
    source = ab.get_source(payload["connector"], config=payload["config"], docker_image=image)
    job_id = os.getenv("WORKER_JOB_ID")
    if job_id:
        # label this job's connector container so a timeout or cancel kills only this one
        # (executable is ['docker', 'run', '--rm', ...])
        source.executor.executable[2:2] = ["--label", f"airbyte-worker-job={job_id}"]
    return source


def op_spec(payload: dict[str, Any]) -> dict[str, Any]:
    """The connector's settings as JSON Schema. Needs no config."""
    return {"connection_specification": make_source({**payload, "config": {}}).config_spec}


def op_check(payload: dict[str, Any]) -> dict[str, Any]:
    make_source(payload).check()
    return {"ok": True}


def _pk(stream: Any) -> list[str]:
    return [path[0] for path in (stream.source_defined_primary_key or []) if path]


class Laps:
    """Seconds per phase of a read, to see where the fixed cost of a run goes."""

    def __init__(self) -> None:
        self.last = time.monotonic()
        self.values: dict[str, Any] = {}

    def mark(self, name: str) -> None:
        now = time.monotonic()
        self.values[name] = round(now - self.last, 2)
        self.last = now


def get_catalog(source: ab.Source, payload: dict[str, Any], names: list[str], refresh: bool = False) -> tuple[Any, str]:
    """The connector's catalog. Discovering starts the connector, so a recent one for the same connector,
    version and config is reused (see catalog_cache). Returns the catalog and 'hit', 'miss' or 'off'."""
    key = catalog_cache.fingerprint(payload["connector"], payload["version"], payload["config"])
    if not refresh:
        cached = catalog_cache.load(key)
        # a stream that is not in the cached catalog may be new in the source: discover again
        if cached and set(names) <= {s["name"] for s in cached.get("streams", [])}:
            source._discovered_catalog = AirbyteCatalog.model_validate(cached)  # undocumented attribute
            return source.discovered_catalog, "hit"
    catalog = source.discovered_catalog
    catalog_cache.save(key, json.loads(catalog.model_dump_json(exclude_none=True)))
    return catalog, ("miss" if catalog_cache.ttl_seconds() > 0 else "off")


def op_discover(payload: dict[str, Any]) -> dict[str, Any]:
    source = make_source(payload)
    streams = []
    for s in get_catalog(source, payload, [], refresh=True)[0].streams:
        streams.append({
            "name": s.name,
            "namespace": s.namespace,
            "primary_key": _pk(s),
            "supported_sync_modes": [str(m.value) for m in s.supported_sync_modes],
            "default_cursor_field": s.default_cursor_field or [],
            "source_defined_cursor": bool(s.source_defined_cursor),
            "json_schema": s.json_schema,
        })
    return {"streams": streams}


def plan_streams(specs: list[dict[str, Any]], discovered: dict[str, dict[str, Any]],
                 default_mode: str) -> dict[str, dict[str, Any]]:
    """Decide per stream: sync mode and cursor. An incremental request for a stream without a cursor
    column falls back to full_refresh (and says so)."""
    plan: dict[str, dict[str, Any]] = {}
    for spec in specs:
        name = spec["name"]
        if name not in discovered:
            raise JobError("config", f"unknown stream: {name}")
        requested = spec.get("mode") or default_mode
        cursor = spec.get("cursor_field") or (discovered[name]["default_cursor_field"] or [None])[0]
        if requested == "incremental" and cursor:
            plan[name] = {"mode": "incremental", "cursor_field": cursor, "fallback": False}
        else:
            plan[name] = {"mode": "full_refresh", "cursor_field": None,
                          "fallback": requested == "incremental"}
    return plan


def _clean(o: Any) -> Any:
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items() if v is not None}
    return [_clean(v) for v in o] if isinstance(o, list) else o


def filter_state(state: list[dict[str, Any]] | None, plan: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Only incremental streams get state in; null fields are stripped (the connector rejects them)."""
    keep = []
    for m in state or []:
        name = m.get("stream", {}).get("stream_descriptor", {}).get("name")
        if name in plan and plan[name]["mode"] == "incremental":
            keep.append(_clean(m))
    return keep


def _temp_file(content: str) -> str:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(suffix=".json", dir=TEMP_DIR)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(content)
    os.chmod(name, 0o644)  # the connector container runs as a non-root user; file is deleted after the read
    return name


def op_read(payload: dict[str, Any], out_dir: Path) -> dict[str, Any]:
    started = time.monotonic()
    laps = Laps()
    read_id = out_dir.name
    source = make_source(payload)
    laps.mark("make_source")
    specs = [{"name": s} if isinstance(s, str) else s for s in payload["streams"]]
    catalog_all, catalog_state = get_catalog(source, payload, [s["name"] for s in specs])
    laps.mark("discover")
    discovered_streams = {s.name: s for s in catalog_all.streams}
    info = {n: {"default_cursor_field": s.default_cursor_field or []} for n, s in discovered_streams.items()}
    plan = plan_streams(specs, info, payload.get("mode", "full_refresh"))

    source.select_streams(list(plan))
    for name, p in plan.items():
        if p["mode"] == "incremental":
            source.set_cursor_key(name, p["cursor_field"])
    catalog = source.get_configured_catalog()
    for cs in catalog.streams:
        if plan[cs.stream.name]["mode"] == "full_refresh":
            cs.sync_mode = SyncMode.full_refresh
            cs.cursor_field = None

    key_fields = {n: (payload.get("key_fields") or {}).get(n) or _pk(discovered_streams[n]) for n in plan}
    schemas = {n: discovered_streams[n].json_schema for n in plan}
    state_in = filter_state(payload.get("state"), plan)

    files = [_temp_file(json.dumps(payload["config"])),
             _temp_file(catalog.model_dump_json(exclude_none=True)),
             _temp_file(json.dumps(state_in))]
    counts = {n: 0 for n in plan}
    key_errors = {n: 0 for n in plan}
    last_state: dict[tuple[str, str | None], dict[str, Any]] = {}
    trace_error: str | None = None
    batch: list[dict[str, Any]] = []
    batches = 0
    bytes_out = 0
    batch_size = int(payload.get("batch_size") or 500)

    def progress() -> None:
        staging.write_json(out_dir / "progress.json",
                           {"counts": counts, "key_errors": key_errors, "batches_ready": batches,
                            "bytes_produced": bytes_out})

    def flush() -> None:
        nonlocal batch, batches, bytes_out
        if batch:
            batches += 1
            bytes_out += staging.write_batch(out_dir, read_id, batches, batch)  # file first, then the progress that names it
            batch = []
            progress()

    progress()
    laps.mark("plan")

    first_line = True
    try:
        lines = source.executor.execute(
            ["read", "--config", files[0], "--catalog", files[1], "--state", files[2]],
            suppress_stderr=True)
        for line in lines:
            if first_line:
                laps.mark("connector_start")  # until the connector's first message
                first_line = False
            try:
                msg = json.loads(line, parse_float=Decimal)
            except ValueError:
                continue  # connector log line
            kind = msg.get("type") if isinstance(msg, dict) else None
            if kind == "RECORD":
                rec = msg["record"]
                name = rec["stream"]
                if name not in plan:
                    continue
                out = canonical.to_output(name, rec["data"], schemas[name], key_fields[name])
                counts[name] += 1
                key_errors[name] += 1 if "key_error" in out else 0
                batch.append(out)
                if len(batch) >= batch_size:
                    flush()
            elif kind == "STATE":
                st = msg["state"]
                desc = st.get("stream", {}).get("stream_descriptor", {})
                if desc.get("name") in plan and plan[desc["name"]]["mode"] == "incremental":
                    # the connector's own message, re-parsed with plain floats so it goes back unchanged
                    last_state[(desc["name"], desc.get("namespace"))] = json.loads(line)["state"]
            elif kind == "TRACE" and msg.get("trace", {}).get("type") == "ERROR":
                trace_error = msg["trace"]["error"].get("message") or "connector error"
    except Exception as exc:  # noqa: BLE001 - classified by the caller
        raise JobError("connector", trace_error or f"connector failed: {type(exc).__name__}") from exc
    finally:
        for f in files:
            Path(f).unlink(missing_ok=True)
    if trace_error:
        raise JobError("connector", trace_error)
    flush()
    progress()
    laps.mark("connector_read")

    new_state = list(last_state.values())  # the connector's own STATE messages, passed through
    staging.write_json(out_dir / "state.json", new_state)
    return {
        "read_id": read_id,
        "counts": counts,
        "key_errors": key_errors,
        "batches": batches,
        "bytes_produced": bytes_out,
        "timings": {**laps.values, "catalog_cache": catalog_state},
        "sync_modes": {n: p["mode"] for n, p in plan.items()},
        "fell_back_to_full_refresh": [n for n, p in plan.items() if p["fallback"]],
        "duration_seconds": round(time.monotonic() - started, 2),
    }
