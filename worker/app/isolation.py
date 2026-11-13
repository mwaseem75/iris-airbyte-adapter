"""Run check / discover / read in a child process with a timeout, so the API can never hang.

On timeout the child's process group is killed and any connector container still running is killed
too (one operation at a time, see main.py). The payload (with credentials) goes through stdin, never argv.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

READ_TIMEOUT_SECONDS = int(os.getenv("READ_TIMEOUT_SECONDS", "120"))


class WorkerError(Exception):
    def __init__(self, category: str, message: str) -> None:
        super().__init__(message)
        self.category = category
        self.message = message


def _kill_connector_containers(connector: str) -> None:
    try:
        ids = subprocess.run(["docker", "ps", "-q", "--filter", f"ancestor={connector}"],
                             capture_output=True, text=True, timeout=15).stdout.split()
        if ids:
            subprocess.run(["docker", "kill", *ids], capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        pass


def run_isolated(op: str, payload: dict[str, Any], work_dir: Path | None = None,
                 timeout: int | None = None) -> dict[str, Any]:
    """Return the op result, or raise WorkerError(category, message). Categories: timeout, auth,
    connection, config, connector, internal."""
    timeout = timeout or READ_TIMEOUT_SECONDS
    own_dir = work_dir is None
    work = work_dir or Path(tempfile.mkdtemp(prefix=f"{op}-", dir=os.getenv("AIRBYTE_TEMP_DIR", "/tmp")))
    image = f"airbyte/{payload['connector']}:{payload['version']}"
    try:
        proc = subprocess.Popen([sys.executable, "-W", "ignore", "-m", "app.read_job", op, str(work)],
                                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, text=True, start_new_session=True)
        try:
            proc.communicate(json.dumps(payload), timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
            _kill_connector_containers(image)
            raise WorkerError("timeout", f"{op} did not finish within {timeout} seconds") from None
        result_file = work / "result.json"
        if not result_file.exists():
            raise WorkerError("internal", f"{op} process ended without a result (exit code {proc.returncode})")
        result = json.loads(result_file.read_text(encoding="utf-8"))
        if not result["ok"]:
            raise WorkerError(result["category"], result["message"])
        return result["result"]
    finally:
        if own_dir:
            shutil.rmtree(work, ignore_errors=True)
