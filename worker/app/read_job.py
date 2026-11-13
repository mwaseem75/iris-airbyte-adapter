"""Child process entry point: python -m app.read_job <op> <work_dir>. Payload (with credentials) on stdin,
result in <work_dir>/result.json. Run in its own process so a hang can be killed (see isolation.py)."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from app import runner, staging


def classify(message: str) -> str:
    m = message.lower()
    if "authentication failed" in m or "password" in m or "permission denied" in m:
        return "auth"
    if any(s in m for s in ("could not connect", "connection refused", "unknownhost", "timed out",
                            "unknown host", "no route", "connection attempt failed")):
        return "connection"
    return "connector"


def short(message: str) -> str:
    """PyAirbyte wraps the real reason in a long banner; keep the reason."""
    m = re.search(r"Failure Reason: '(.*?)'\n", message, re.S)
    return m.group(1) if m else message


def scrub(message: str, config: dict) -> str:
    message = short(message)
    for key, value in config.items():
        if isinstance(value, str) and value and any(w in key for w in ("password", "secret", "token", "key")):
            message = message.replace(value, "***")
    return message[:500]


def main() -> None:
    op, work = sys.argv[1], Path(sys.argv[2])
    payload = json.loads(sys.stdin.read())
    config = payload.get("config", {})
    try:
        if op == "check":
            result = runner.op_check(payload)
        elif op == "discover":
            result = runner.op_discover(payload)
        else:
            result = runner.op_read(payload, work)
        staging.write_json(work / "result.json", {"ok": True, "result": result})
    except runner.JobError as e:
        category = e.category if e.category != "connector" else classify(e.message)
        staging.write_json(work / "result.json",
                           {"ok": False, "category": category, "message": scrub(e.message, config)})
    except Exception as e:  # noqa: BLE001
        text = str(e)
        staging.write_json(work / "result.json",
                           {"ok": False, "category": classify(text), "message": scrub(text, config)})


if __name__ == "__main__":
    main()
