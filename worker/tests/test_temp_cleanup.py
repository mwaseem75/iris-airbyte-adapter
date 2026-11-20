"""Temp files hold credentials: they must be gone after every job, including killed ones."""
import subprocess

import pytest

from app import isolation
from app.isolation import WorkerError


class FakeProc:
    pid = 999999

    def __init__(self, cmd, env, **kw):
        # the child writes a file with a secret into its temp folder, like PyAirbyte does
        (isolation.Path(env["TMPDIR"]) / "config.json").write_text('{"password": "S3CRET"}')

    def communicate(self, data, timeout=None):
        raise subprocess.TimeoutExpired("x", timeout)

    def wait(self):
        return -9


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(isolation, "TEMP_ROOT", tmp_path)
    monkeypatch.setattr(isolation.subprocess, "Popen", lambda cmd, **kw: FakeProc(cmd, kw["env"]))
    monkeypatch.setattr(isolation, "_kill_group", lambda proc: None)
    monkeypatch.setattr(isolation, "_kill_job_containers", lambda job_id: None)
    return tmp_path


def test_killed_job_leaves_no_temp_files(root, tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    with pytest.raises(WorkerError) as e:
        isolation.run_isolated("read", {"connector": "c", "version": "1"}, work_dir=work, timeout=1, job_id="r-1")
    assert e.value.category == "timeout"
    assert not (root / "job-r-1").exists()
    assert not list(root.rglob("config.json"))


def test_cancelled_before_start_also_cleans_up(root, tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    isolation.cancel("r-2")
    with pytest.raises(WorkerError):
        isolation.run_isolated("read", {"connector": "c", "version": "1"}, work_dir=work, timeout=1, job_id="r-2")
    assert not (root / "job-r-2").exists()


def test_sweep_removes_leftovers_from_earlier_processes(root):
    (root / "job-old").mkdir()
    (root / "job-old" / "config.json").write_text("secret")
    (root / "tmpabc123.json").write_text("secret")
    (root / "keep.txt").write_text("not ours")
    assert isolation.sweep_temp() == 2
    assert [p.name for p in root.iterdir()] == ["keep.txt"]
