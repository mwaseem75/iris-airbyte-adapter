"""Async read API without Docker: run_isolated is replaced by a fake that writes batches like op_read does."""
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app import main, staging
from app.isolation import WorkerError

BODY = {"config": {"host": "x"}, "streams": ["items"], "batch_size": 10}


class FakeRead:
    """Writes batch 1, waits until `proceed` is set (or the read is cancelled), then writes batch 2."""

    def __init__(self, fail: WorkerError | None = None) -> None:
        self.proceed = threading.Event()
        self.cancelled: set[str] = set()
        self.fail = fail

    def _batch(self, d, read_id, n):
        staging.write_batch(d, read_id, n, [{"stream": "items", "key": [str(n)], "data": {"id": n}}])
        staging.write_json(d / "progress.json", {"counts": {"items": n * 10}, "key_errors": {"items": 0},
                                                 "batches_ready": n})

    def run(self, op, payload, work_dir=None, timeout=None, job_id=None):
        assert "wait" not in payload and "connection_id" not in payload
        self._batch(work_dir, job_id, 1)
        while not self.proceed.wait(0.02):
            if job_id in self.cancelled:
                raise WorkerError("cancelled", "read was cancelled")
        if self.fail:
            raise self.fail
        self._batch(work_dir, job_id, 2)
        staging.write_json(work_dir / "state.json", [{"type": "STREAM"}])
        return {"read_id": job_id, "counts": {"items": 20}, "key_errors": {"items": 0}, "batches": 2,
                "sync_modes": {"items": "full_refresh"}, "fell_back_to_full_refresh": [], "duration_seconds": 0.1}

    def cancel(self, job_id):
        self.cancelled.add(job_id)


@pytest.fixture
def api(tmp_path, monkeypatch):
    monkeypatch.setattr(staging, "READS_DIR", tmp_path)
    monkeypatch.setattr(main, "MAX_CONCURRENT_READS", 2)
    monkeypatch.setattr(main, "_slots_used", 0)
    monkeypatch.setattr(main, "_connections", set())
    fake = FakeRead()
    monkeypatch.setattr(main, "run_isolated", fake.run)
    monkeypatch.setattr(main, "cancel_job", fake.cancel)
    client = TestClient(main.app)
    client.fake = fake
    yield client
    fake.proceed.set()  # let leftover threads end
    wait_for(lambda: main._slots_used == 0)


def wait_for(cond, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


def start(api, **extra):
    return api.post("/reads", json={**BODY, "wait": False, **extra})


def status(api, rid):
    return api.get(f"/reads/{rid}").json()


def test_wait_false_returns_202_at_once_then_completes(api):
    t0 = time.monotonic()
    r = start(api)
    assert r.status_code == 202 and time.monotonic() - t0 < 1
    rid = r.json()["read_id"]
    assert r.json()["status"] == "running"
    assert wait_for(lambda: status(api, rid).get("batches_ready") == 1)
    s = status(api, rid)
    assert s["status"] == "running" and s["counts"] == {"items": 10}
    api.fake.proceed.set()
    assert wait_for(lambda: status(api, rid)["status"] == "completed")
    s = status(api, rid)
    assert s["batches"] == 2 and s["counts"] == {"items": 20} and s["sync_modes"] == {"items": "full_refresh"}
    state = api.get(f"/reads/{rid}/state").json()
    assert state["committed"] is False and state["state"] == [{"type": "STREAM"}]


def test_batch_available_while_running_and_not_ready_beyond(api):
    rid = start(api).json()["read_id"]
    assert wait_for(lambda: status(api, rid).get("batches_ready") == 1)
    b1 = api.get(f"/reads/{rid}/batches/1")
    assert b1.status_code == 200 and b1.json()["records"][0]["key"] == ["1"]
    b2 = api.get(f"/reads/{rid}/batches/2")
    assert b2.status_code == 409 and b2.json()["error"]["category"] == "not_ready"
    api.fake.proceed.set()
    assert wait_for(lambda: status(api, rid)["status"] == "completed")
    assert api.get(f"/reads/{rid}/batches/2").status_code == 200
    assert api.get(f"/reads/{rid}/batches/3").status_code == 404


def test_state_not_available_while_running(api):
    rid = start(api).json()["read_id"]
    assert api.get(f"/reads/{rid}/state").status_code == 404


def test_cancel_stops_read_and_gives_no_state(api):
    rid = start(api, connection_id="c1").json()["read_id"]
    assert wait_for(lambda: status(api, rid).get("batches_ready") == 1)
    r = api.delete(f"/reads/{rid}")
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    assert wait_for(lambda: main._slots_used == 0)
    assert status(api, rid)["status"] == "cancelled"  # the reader thread did not overwrite it
    assert api.get(f"/reads/{rid}/state").status_code == 404
    assert api.get(f"/reads/{rid}/batches/1").status_code == 200  # what was written stays readable
    assert api.get(f"/reads/{rid}/batches/2").status_code == 404
    assert api.delete(f"/reads/{rid}").json()["error"]["category"] == "not_running"
    assert start(api, connection_id="c1").status_code == 202  # connection is free again


def test_same_connection_gets_409_busy(api):
    first = start(api, connection_id="conn-a")
    assert first.status_code == 202
    second = start(api, connection_id="conn-a")
    assert second.status_code == 409 and second.json()["error"]["category"] == "busy"
    assert start(api, connection_id="conn-b").status_code == 202


def test_global_limit_counts_all_reads_and_operations(api):
    assert start(api).status_code == 202
    assert start(api).status_code == 202
    third = start(api)
    assert third.status_code == 409 and third.json()["error"]["category"] == "busy"
    # check and discover share the limit
    assert api.post("/check", json={"config": {}}).status_code == 409
    assert api.post("/discover", json={"config": {}}).status_code == 409


def test_wait_true_returns_full_result(api):
    api.fake.proceed.set()
    r = api.post("/reads", json=BODY)
    assert r.status_code == 200
    assert r.json()["batches"] == 2 and r.json()["counts"] == {"items": 20}
    assert main._slots_used == 0
    assert status(api, r.json()["read_id"])["status"] == "completed"


def test_timeout_gives_failed_status_with_category(api):
    api.fake.proceed.set()
    api.fake.fail = WorkerError("timeout", "read did not finish within 1 seconds")
    rid = start(api).json()["read_id"]
    assert wait_for(lambda: status(api, rid)["status"] == "failed")
    s = status(api, rid)
    assert s["error"]["category"] == "timeout"
    assert api.get(f"/reads/{rid}/batches/9").status_code == 404
    # wait=true gives the HTTP error as before
    r = api.post("/reads", json=BODY)
    assert r.status_code == 504 and r.headers["X-Read-Id"]


def test_unknown_read(api):
    assert api.get("/reads/r-nope").status_code == 404
    assert api.delete("/reads/r-nope").status_code == 404
    assert api.get("/reads/r-nope/batches/1").status_code == 404
