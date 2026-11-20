"""Run check / discover / read in a child process with a timeout, so the API can never hang.

On timeout or cancel the child's process group is killed, and so is the connector container that job
started. The container is found by a label (job id) that the child puts on its `docker run`, so other
jobs running the same image are never touched. The payload (with credentials) goes through stdin, never argv.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any

READ_TIMEOUT_SECONDS = int(os.getenv("READ_TIMEOUT_SECONDS", "120"))


class WorkerError(Exception):
    def __init__(self, category: str, message: str) -> None:
        super().__init__(message)
        self.category = category
        self.message = message


TEMP_ROOT = Path(os.getenv("AIRBYTE_TEMP_DIR", "/tmp/airbyte/tmp"))
JOB_LABEL = "airbyte-worker-job"
JOB_ENV = "WORKER_JOB_ID"

_lock = threading.Lock()
_procs: dict[str, subprocess.Popen[str]] = {}
_cancelled: set[str] = set()


def _kill_job_containers(job_id: str) -> None:
    try:
        ids = subprocess.run(["docker", "ps", "-q", "--filter", f"label={JOB_LABEL}={job_id}"],
                             capture_output=True, text=True, timeout=15).stdout.split()
        if ids:
            subprocess.run(["docker", "kill", *ids], capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        pass


def _kill_group(proc: subprocess.Popen[str]) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def job_temp_dir(job_id: str) -> Path:
    """Every job gets its own temp folder (config, catalog and state files with credentials go there), removed
    when the job ends however it ends. A killed process cannot clean up after itself."""
    return TEMP_ROOT / f"job-{job_id}"


def sweep_temp() -> int:
    """At startup nothing is running: remove job folders and loose temp files a killed process left behind."""
    removed = 0
    if TEMP_ROOT.exists():
        for p in list(TEMP_ROOT.glob("job-*")) + list(TEMP_ROOT.glob("tmp*.json")):
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            else:
                p.unlink(missing_ok=True)
            removed += 1
    return removed


def cancel(job_id: str) -> None:
    """Stop a running job: kill its process group and its connector container. Safe to call early
    (before the process exists) or late."""
    with _lock:
        _cancelled.add(job_id)
        proc = _procs.get(job_id)
    if proc:
        _kill_group(proc)
    _kill_job_containers(job_id)


def run_isolated(op: str, payload: dict[str, Any], work_dir: Path | None = None,
                 timeout: int | None = None, job_id: str | None = None) -> dict[str, Any]:
    """Return the op result, or raise WorkerError(category, message). Categories: timeout, cancelled, auth,
    connection, config, connector, internal."""
    timeout = timeout or READ_TIMEOUT_SECONDS
    job_id = job_id or uuid.uuid4().hex[:12]
    own_dir = work_dir is None
    work = work_dir or Path(tempfile.mkdtemp(prefix=f"{op}-", dir=os.getenv("AIRBYTE_TEMP_DIR", "/tmp")))
    tmp = job_temp_dir(job_id)
    try:
        tmp.mkdir(parents=True, exist_ok=True)
        tmp.chmod(0o755)  # the connector container runs as another user and must read the files in it
        proc = subprocess.Popen([sys.executable, "-W", "ignore", "-m", "app.read_job", op, str(work)],
                                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, text=True, start_new_session=True,
                                env={**os.environ, JOB_ENV: job_id, "TMPDIR": str(tmp), "AIRBYTE_TEMP_DIR": str(tmp)})
        with _lock:
            _procs[job_id] = proc
            was_cancelled = job_id in _cancelled
        if was_cancelled:
            _kill_group(proc)
        try:
            proc.communicate(json.dumps(payload), timeout=timeout)
        except subprocess.TimeoutExpired:
            _kill_group(proc)
            proc.wait()
            _kill_job_containers(job_id)
            raise WorkerError("timeout", f"{op} did not finish within {timeout} seconds") from None
        with _lock:
            was_cancelled = job_id in _cancelled
        if was_cancelled:
            _kill_job_containers(job_id)
            raise WorkerError("cancelled", f"{op} was cancelled")
        result_file = work / "result.json"
        if not result_file.exists():
            raise WorkerError("internal", f"{op} process ended without a result (exit code {proc.returncode})")
        result = json.loads(result_file.read_text(encoding="utf-8"))
        if not result["ok"]:
            raise WorkerError(result["category"], result["message"])
        return result["result"]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        with _lock:
            _procs.pop(job_id, None)
            _cancelled.discard(job_id)
        if own_dir:
            shutil.rmtree(work, ignore_errors=True)
