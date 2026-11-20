"""Per-read working folders under /data/reads/<read_id>/. A folder is never reused for another read."""
from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

READS_DIR = Path(os.getenv("READS_DIR", "/data/reads"))
KEEP_READS = int(os.getenv("KEEP_READS", "10"))


def new_read_id() -> str:
    return f"r-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def read_dir(read_id: str) -> Path:
    return READS_DIR / read_id


def create(read_id: str) -> Path:
    """Create a fresh folder for this read and delete the oldest ones beyond KEEP_READS."""
    READS_DIR.mkdir(parents=True, exist_ok=True)
    folders = sorted((p for p in READS_DIR.iterdir() if p.is_dir()), key=lambda p: p.name)
    for old in folders[: max(0, len(folders) - (KEEP_READS - 1))]:
        shutil.rmtree(old, ignore_errors=True)
    d = read_dir(read_id)
    (d / "batches").mkdir(parents=True)
    return d


def write_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def write_batch(d: Path, read_id: str, n: int, records: list[dict[str, Any]]) -> int:
    """Write one batch file; returns its size in bytes."""
    path = d / "batches" / f"{n}.json"
    write_json(path, {"read_id": read_id, "batch": n, "records": records})
    return path.stat().st_size
