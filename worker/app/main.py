"""Worker API. Credentials arrive in request bodies. They are never
logged; validation errors never echo the request body."""
from __future__ import annotations

import functools
import importlib.metadata as md
import logging
import os
import platform
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app import spec as specs, staging
from app.isolation import READ_TIMEOUT_SECONDS, WorkerError, cancel as cancel_job, run_isolated, sweep_temp

DEFAULT_CONNECTOR = "source-postgres"
DEFAULT_VERSION = "3.8.11"
ALLOWED_CONNECTORS = {c.strip() for c in os.getenv("ALLOWED_CONNECTORS", DEFAULT_CONNECTOR).split(",") if c.strip()}  # only tested connectors

# limit for all connector operations together: reads, check and discover
MAX_CONCURRENT_READS = max(1, int(os.getenv("MAX_CONCURRENT_READS", "2")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("worker")
app = FastAPI(title="iris-airbyte-adapter worker")

_slots_lock = threading.Lock()
_slots_used = 0
_connections: set[str] = set()  # connection ids with an active read
_status_lock = threading.Lock()  # status.json is rewritten by the read thread and by cancel

STATUS_FOR = {"timeout": 504, "busy": 409, "not_found": 404, "config": 400, "validation": 400,
              "not_ready": 409, "not_running": 409, "not_completed": 409, "cancelled": 409}


class SourceRequest(BaseModel):
    connector: str = DEFAULT_CONNECTOR
    version: str = DEFAULT_VERSION
    config: dict[str, Any]


class SpecRequest(BaseModel):
    connector: str = DEFAULT_CONNECTOR
    version: str = DEFAULT_VERSION


class StreamSpec(BaseModel):
    name: str
    mode: Literal["full_refresh", "incremental"] | None = None
    cursor_field: str | None = None


class ReadRequest(SourceRequest):
    streams: list[StreamSpec | str] = Field(min_length=1)
    mode: Literal["full_refresh", "incremental"] = "full_refresh"
    batch_size: int = Field(default=500, ge=1, le=10000)
    state: list[dict[str, Any]] | None = None
    key_fields: dict[str, list[str]] = {}
    connection_id: str | None = None  # at most one active read per connection
    wait: bool = True  # false: answer 202 at once and read in the background


class ConfigProblems(WorkerError):
    """Config does not match the connector's spec. details: [{path, reason}], never values."""

    def __init__(self, details: list[dict[str, str]]) -> None:
        super().__init__("config", "config does not match the connector spec")
        self.details = details


def _error(category: str, message: str, status: int | None = None,
           details: list[dict[str, str]] | None = None) -> JSONResponse:
    body: dict[str, Any] = {"category": category, "message": message}
    if details:
        body["details"] = details
    return JSONResponse({"error": body}, status_code=status or STATUS_FOR.get(category, 502))


def _worker_error(e: WorkerError) -> JSONResponse:
    return _error(e.category, e.message, details=getattr(e, "details", None))


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    # default handler echoes the input, which may hold credentials
    problems = [{"loc": [str(x) for x in e["loc"]], "msg": e["msg"]} for e in exc.errors()]
    return JSONResponse({"error": {"category": "validation", "message": "invalid request", "details": problems}},
                        status_code=422)


def _payload(req: SourceRequest) -> dict[str, Any]:
    if req.connector not in ALLOWED_CONNECTORS:
        raise WorkerError("config", f"connector not supported: {req.connector}")
    return req.model_dump(exclude={"wait", "connection_id"})


def _spec(req: SourceRequest) -> dict[str, Any] | None:
    """The connector's spec (cached per image). None if it cannot be fetched."""
    try:
        return specs.get_spec(req.connector, req.version,
                              lambda: run_isolated("spec", {"connector": req.connector, "version": req.version}))
    except (WorkerError, ValueError) as e:
        log.warning("spec for %s:%s not available: %s", req.connector, req.version, type(e).__name__)
        return None


def _prepare(req: SourceRequest) -> dict[str, Any]:
    """Payload for the child process. Raises ConfigProblems if the config does not fit the spec.
    Call with a slot held (fetching the spec runs the connector once per image)."""
    payload = _payload(req)
    spec = _spec(req)
    if spec:
        found = specs.problems(req.config, spec["connection_specification"])
        if found:
            log.warning("config rejected for %s: %d problem(s) at %s", req.connector, len(found),
                        [p["path"] for p in found])
            raise ConfigProblems(found)
        payload["secret_fields"] = spec["secret_fields"]
    return payload


def _acquire(connection_id: str | None = None) -> JSONResponse | None:
    """Take a slot (and the connection, if given). Returns a 409 busy response when that is not possible."""
    global _slots_used
    with _slots_lock:
        if connection_id and connection_id in _connections:
            return _error("busy", "a read for this connection is already running")
        if _slots_used >= MAX_CONCURRENT_READS:
            return _error("busy", f"worker is at its limit of {MAX_CONCURRENT_READS} concurrent operations")
        _slots_used += 1
        if connection_id:
            _connections.add(connection_id)
    return None


def _release(connection_id: str | None = None) -> None:
    global _slots_used
    with _slots_lock:
        _slots_used -= 1
        if connection_id:
            _connections.discard(connection_id)


def _limited(fn):  # type: ignore[no-untyped-def]
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        busy = _acquire()
        if busy:
            return busy
        try:
            return fn(*args, **kwargs)
        except WorkerError as e:
            return _worker_error(e)
        finally:
            _release()
    return wrapper


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/env")
def env() -> dict[str, Any]:
    def ver(pkg: str) -> str:
        try:
            return md.version(pkg)
        except md.PackageNotFoundError:
            return "not installed"
    docker = subprocess.run(["docker", "--version"], capture_output=True, text=True).stdout.strip()
    return {
        "python": platform.python_version(),
        "pyairbyte": ver("airbyte"),
        "airbyte_cdk": ver("airbyte-cdk"),
        "duckdb": ver("duckdb"),
        "fastapi": ver("fastapi"),
        "docker_cli": docker,
        "connector": {"name": DEFAULT_CONNECTOR, "image": f"airbyte/{DEFAULT_CONNECTOR}:{DEFAULT_VERSION}"},
        "read_timeout_seconds": READ_TIMEOUT_SECONDS,
        "max_concurrent_reads": MAX_CONCURRENT_READS,
        "reads_kept": staging.KEEP_READS,
    }


@app.post("/spec")
@_limited
def spec(req: SpecRequest) -> Any:
    _payload(req)  # connector must be allowed
    found = _spec(req)
    if not found:
        raise WorkerError("connector", "could not read the connector spec")
    return found


@app.post("/check")
@_limited
def check(req: SourceRequest) -> Any:
    payload = _prepare(req)  # a config that does not fit the spec is a 400, not an {ok: false}
    try:
        run_isolated("check", payload)
        return {"ok": True}
    except WorkerError as e:
        return {"ok": False, "error": {"category": e.category, "message": e.message}}


@app.post("/discover")
@_limited
def discover(req: SourceRequest) -> Any:
    return run_isolated("discover", _prepare(req))


def _update_status(d: Path, **fields: Any) -> None:
    """Rewrite status.json; a cancelled read stays cancelled."""
    with _status_lock:
        status = staging.read_json(d / "status.json") or {}
        if status.get("status") != "cancelled":
            status |= fields
            staging.write_json(d / "status.json", status)


def _batches_ready(d: Path) -> int:
    return (staging.read_json(d / "progress.json") or {}).get("batches_ready", 0)


def _execute_read(read_id: str, d: Path, payload: dict[str, Any], connection_id: str | None) -> dict[str, Any]:
    """Run the read, write the final status, give the slot back. Returns the result, or {'error': ...}."""
    t0 = time.monotonic()
    try:
        result = run_isolated("read", payload, work_dir=d, job_id=read_id)
        # the job's own figure leaves out starting the child process (importing PyAirbyte takes seconds):
        # keep it as read_seconds and report the whole time the worker spent as duration_seconds
        result["read_seconds"] = result.get("duration_seconds")
        result["duration_seconds"] = round(time.monotonic() - t0, 2)
        _update_status(d, status="completed", **result)
        log.info("read %s completed: counts=%s batches=%s in %ss", read_id, result["counts"],
                 result["batches"], result["duration_seconds"])
        return result
    except WorkerError as e:
        outcome = "cancelled" if e.category == "cancelled" else "failed"
        fields: dict[str, Any] = {"status": outcome, "duration_seconds": round(time.monotonic() - t0, 2)}
        if outcome == "failed":
            fields["error"] = {"category": e.category, "message": e.message}
        _update_status(d, **fields)
        log.warning("read %s %s: category=%s batches=%s in %ss", read_id, outcome, e.category,
                    _batches_ready(d), fields["duration_seconds"])
        return {"error": {"category": e.category, "message": e.message}}
    except Exception as e:  # noqa: BLE001 - never leave a read stuck in "running"
        _update_status(d, status="failed", error={"category": "internal", "message": type(e).__name__},
                       duration_seconds=round(time.monotonic() - t0, 2))
        log.error("read %s failed: internal (%s)", read_id, type(e).__name__)
        return {"error": {"category": "internal", "message": type(e).__name__}}
    finally:
        _release(connection_id)


@app.post("/reads")
def start_read(req: ReadRequest) -> Any:
    try:
        _payload(req)  # connector allowed? (cheap, before taking a slot)
    except WorkerError as e:
        return _worker_error(e)
    busy = _acquire(req.connection_id)
    if busy:
        return busy
    try:
        payload = _prepare(req)
        read_id = staging.new_read_id()
        d = staging.create(read_id)
        staging.write_json(d / "status.json", {"read_id": read_id, "status": "running",
                                               "started_at": datetime.now(timezone.utc).isoformat()})
        log.info("read %s started: streams=%d mode=%s wait=%s", read_id, len(req.streams), req.mode, req.wait)
        if not req.wait:
            threading.Thread(target=_execute_read, args=(read_id, d, payload, req.connection_id),
                             name=read_id, daemon=True).start()
            return JSONResponse({"read_id": read_id, "status": "running"}, status_code=202)
    except WorkerError as e:  # config problems: the read never started
        _release(req.connection_id)
        return _worker_error(e)
    except BaseException:
        _release(req.connection_id)  # the read never started: give the slot back
        raise
    result = _execute_read(read_id, d, payload, req.connection_id)  # releases the slot itself
    if "error" in result:
        resp = _error(result["error"]["category"], result["error"]["message"])
        resp.headers["X-Read-Id"] = read_id
        return resp
    return result


def _status_of(read_id: str) -> dict[str, Any] | None:
    return staging.read_json(staging.read_dir(read_id) / "status.json") if _valid_id(read_id) else None


@app.get("/reads/{read_id}")
def read_status(read_id: str) -> Any:
    status = _status_of(read_id)
    if not status:
        return _error("not_found", f"unknown read: {read_id}")
    progress = staging.read_json(staging.read_dir(read_id) / "progress.json") or {}
    return {**progress, **status}  # final values in status.json win


@app.delete("/reads/{read_id}")
def cancel_read(read_id: str) -> Any:
    status = _status_of(read_id)
    if not status:
        return _error("not_found", f"unknown read: {read_id}")
    if status.get("status") != "running":
        return _error("not_running", f"read {read_id} is not running (status: {status.get('status')})")
    d = staging.read_dir(read_id)
    _update_status(d, status="cancelled")  # first, so the reader thread cannot overwrite it
    cancel_job(read_id)
    log.info("read %s cancelled by request, batches=%s", read_id, _batches_ready(d))
    return read_status(read_id)


@app.get("/reads/{read_id}/batches/{n}")
def read_batch(read_id: str, n: int) -> Any:
    status = _status_of(read_id)  # read before the batch: a batch written in between just reads as "not ready"
    batch = staging.read_json(staging.read_dir(read_id) / "batches" / f"{n}.json") if status else None
    if batch:
        return batch
    if status and status.get("status") == "running":
        return _error("not_ready", f"batch {n} of read {read_id} is not written yet")
    return _error("not_found", f"no batch {n} for read {read_id}")


@app.post("/reads/{read_id}/ack")
def ack_read(read_id: str) -> Any:
    """The caller has stored every batch and the state: delete the batch files (data minimisation).
    status.json, progress.json and state.json stay."""
    status = _status_of(read_id)
    if not status:
        return _error("not_found", f"unknown read: {read_id}")
    if status.get("status") != "completed":
        return _error("not_completed", f"read {read_id} is not completed (status: {status.get('status')})")
    deleted = 0
    for f in (staging.read_dir(read_id) / "batches").glob("*.json"):
        f.unlink(missing_ok=True)
        deleted += 1
    log.info("read %s acknowledged: %d batch files deleted", read_id, deleted)
    return {"read_id": read_id, "deleted_batches": deleted}


@app.get("/reads/{read_id}/state")
def read_state(read_id: str) -> Any:
    status = _status_of(read_id)
    if not status:
        return _error("not_found", f"unknown read: {read_id}")
    if status.get("status") != "completed":
        return _error("not_found", f"read {read_id} has no state (status: {status.get('status')})")
    # always false: only the caller knows when the data is committed, and saves the state after that
    return {"read_id": read_id, "committed": False,
            "state": staging.read_json(staging.read_dir(read_id) / "state.json") or []}


@app.on_event("startup")
def mark_orphaned_reads() -> None:
    """A read left 'running' by a worker restart can never finish: say so. Also remove temp files
    (they hold credentials) that a killed job left behind."""
    log.info("startup: removed %d leftover temp item(s)", sweep_temp())
    if not staging.READS_DIR.exists():
        return
    for d in staging.READS_DIR.iterdir():
        status = staging.read_json(d / "status.json") if d.is_dir() else None
        if status and status.get("status") == "running":
            _update_status(d, status="failed",
                           error={"category": "internal", "message": "worker restarted during the read"})


def _valid_id(read_id: str) -> bool:
    return read_id.startswith("r-") and "/" not in read_id and "\\" not in read_id and ".." not in read_id
