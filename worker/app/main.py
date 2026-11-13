"""Worker API. Credentials arrive in request bodies. They are never
logged; validation errors never echo the request body."""
from __future__ import annotations

import functools
import importlib.metadata as md
import logging
import platform
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app import staging
from app.isolation import READ_TIMEOUT_SECONDS, WorkerError, run_isolated

DEFAULT_CONNECTOR = "source-postgres"
DEFAULT_VERSION = "3.8.11"
ALLOWED_CONNECTORS = {DEFAULT_CONNECTOR}  # only connectors that have been tested

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("worker")
app = FastAPI(title="iris-airbyte-adapter worker")
_busy = threading.Lock()  # one connector operation at a time

STATUS_FOR = {"timeout": 504, "busy": 409, "not_found": 404, "config": 400, "validation": 400}


class SourceRequest(BaseModel):
    connector: str = DEFAULT_CONNECTOR
    version: str = DEFAULT_VERSION
    config: dict[str, Any]


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


def _error(category: str, message: str, status: int | None = None) -> JSONResponse:
    return JSONResponse({"error": {"category": category, "message": message}},
                        status_code=status or STATUS_FOR.get(category, 502))


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    # default handler echoes the input, which may hold credentials
    problems = [{"loc": [str(x) for x in e["loc"]], "msg": e["msg"]} for e in exc.errors()]
    return JSONResponse({"error": {"category": "validation", "message": "invalid request", "details": problems}},
                        status_code=422)


def _payload(req: SourceRequest) -> dict[str, Any]:
    if req.connector not in ALLOWED_CONNECTORS:
        raise WorkerError("config", f"connector not supported: {req.connector}")
    return req.model_dump()


def _locked(fn):  # type: ignore[no-untyped-def]
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        if not _busy.acquire(blocking=False):
            return _error("busy", "another operation is running")
        try:
            return fn(*args, **kwargs)
        except WorkerError as e:
            return _error(e.category, e.message)
        finally:
            _busy.release()
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
        "reads_kept": staging.KEEP_READS,
    }


@app.post("/check")
@_locked
def check(req: SourceRequest) -> Any:
    try:
        run_isolated("check", _payload(req))
        return {"ok": True}
    except WorkerError as e:
        return {"ok": False, "error": {"category": e.category, "message": e.message}}


@app.post("/discover")
@_locked
def discover(req: SourceRequest) -> Any:
    return run_isolated("discover", _payload(req))


@app.post("/reads")
@_locked
def start_read(req: ReadRequest) -> Any:
    payload = _payload(req)
    read_id = staging.new_read_id()
    d = staging.create(read_id)
    started = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    status: dict[str, Any] = {"read_id": read_id, "status": "running", "started_at": started}
    staging.write_json(d / "status.json", status)
    log.info("read %s started: streams=%d mode=%s", read_id, len(req.streams), req.mode)
    try:
        result = run_isolated("read", payload, work_dir=d)
    except WorkerError as e:
        status |= {"status": "failed", "error": {"category": e.category, "message": e.message},
                   "duration_seconds": round(time.monotonic() - t0, 2)}
        staging.write_json(d / "status.json", status)
        log.warning("read %s failed: %s", read_id, e.category)
        resp = _error(e.category, e.message)
        resp.headers["X-Read-Id"] = read_id
        return resp
    status |= {"status": "completed", **result}
    staging.write_json(d / "status.json", status)
    log.info("read %s completed: counts=%s batches=%s in %ss", read_id, result["counts"],
             result["batches"], result["duration_seconds"])
    return result


@app.get("/reads/{read_id}")
def read_status(read_id: str) -> Any:
    status = staging.read_json(staging.read_dir(read_id) / "status.json") if _valid_id(read_id) else None
    return status if status else _error("not_found", f"unknown read: {read_id}")


@app.get("/reads/{read_id}/batches/{n}")
def read_batch(read_id: str, n: int) -> Any:
    batch = staging.read_json(staging.read_dir(read_id) / "batches" / f"{n}.json") if _valid_id(read_id) else None
    return batch if batch else _error("not_found", f"no batch {n} for read {read_id}")


@app.get("/reads/{read_id}/state")
def read_state(read_id: str) -> Any:
    d = staging.read_dir(read_id)
    status = staging.read_json(d / "status.json") if _valid_id(read_id) else None
    if not status:
        return _error("not_found", f"unknown read: {read_id}")
    if status.get("status") != "completed":
        return _error("not_found", f"read {read_id} has no state (status: {status.get('status')})")
    # always false: only the caller knows when the data is committed, and saves the state after that
    return {"read_id": read_id, "committed": False, "state": staging.read_json(d / "state.json") or []}


def _valid_id(read_id: str) -> bool:
    return read_id.startswith("r-") and "/" not in read_id and "\\" not in read_id and ".." not in read_id
