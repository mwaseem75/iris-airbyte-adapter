"""Spec handling without Docker: secret fields, validation, scrubbing, caching, API errors."""
import json

import pytest
from fastapi.testclient import TestClient

from app import main, read_job, spec as specs

SPEC = {
    "type": "object",
    "required": ["host", "port", "username"],
    "properties": {
        "host": {"type": "string"},
        "port": {"type": "integer"},
        "username": {"type": "string"},
        "password": {"type": "string", "airbyte_secret": True},
        "ssl_mode": {"oneOf": [
            {"title": "disable", "properties": {"mode": {"const": "disable"}}},
            {"title": "verify", "properties": {"mode": {"const": "verify"},
                                              "client_key": {"type": "string", "airbyte_secret": True}}},
        ]},
        "tunnel_method": {"oneOf": [
            {"properties": {"tunnel_method": {"const": "NO_TUNNEL"}}},
            {"properties": {"tunnel_method": {"const": "SSH_KEY_AUTH"},
                            "ssh_key": {"type": "string", "airbyte_secret": True}}},
        ]},
    },
}
GOOD = {"host": "h", "port": 5432, "username": "u", "password": "pw-1"}


def test_secret_fields_include_nested_one_of_options():
    assert specs.secret_fields(SPEC) == ["password", "ssl_mode.client_key", "tunnel_method.ssh_key"]


def test_valid_config_has_no_problems():
    assert specs.problems(GOOD, SPEC) == []
    assert specs.problems({**GOOD, "ssl_mode": {"mode": "disable"}}, SPEC) == []


def test_problems_give_path_and_reason_but_never_values():
    bad = {"port": "not-a-number-S3CRET", "username": "u", "password": "S3CRET-pw",
           "ssl_mode": {"mode": "bogus-S3CRET"}}
    found = specs.problems(bad, SPEC)
    paths = {p["path"] for p in found}
    assert "host" in paths and "port" in paths and "ssl_mode" in paths or any(p.startswith("ssl_mode") for p in paths)
    assert "S3CRET" not in json.dumps(found)


def test_get_spec_is_cached_in_memory_and_in_a_file(tmp_path, monkeypatch):
    monkeypatch.setattr(specs, "SPECS_DIR", tmp_path)
    monkeypatch.setattr(specs, "_cache", {})
    calls = []
    fetch = lambda: calls.append(1) or {"connection_specification": SPEC}  # noqa: E731
    first = specs.get_spec("source-x", "1.0", fetch)
    assert specs.get_spec("source-x", "1.0", fetch) is first and len(calls) == 1
    assert (tmp_path / "source-x-1.0.json").exists()
    monkeypatch.setattr(specs, "_cache", {})  # new process: read the file, do not fetch again
    assert specs.get_spec("source-x", "1.0", fetch)["secret_fields"] == first["secret_fields"] and len(calls) == 1


def test_get_spec_rejects_unsafe_names():
    with pytest.raises(ValueError):
        specs.get_spec("../etc", "1", lambda: {})


def test_scrub_uses_spec_secret_fields():
    config = {"password": "pw-1", "tunnel_method": {"ssh_key": "KEY-VALUE"}, "token_name": "visible"}
    msg = "login failed for pw-1 using KEY-VALUE and visible"
    out = read_job.scrub(msg, config, ["password", "tunnel_method.ssh_key"])
    assert "pw-1" not in out and "KEY-VALUE" not in out and "visible" in out


def test_scrub_falls_back_to_key_names_without_spec():
    out = read_job.scrub("bad password pw-1", {"password": "pw-1", "host": "h"})
    assert "pw-1" not in out


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main, "_slots_used", 0)
    monkeypatch.setattr(main, "_spec", lambda req: {"connector": req.connector, "version": req.version,
                                                    "connection_specification": SPEC,
                                                    "secret_fields": specs.secret_fields(SPEC)})
    seen = []
    monkeypatch.setattr(main, "run_isolated", lambda op, payload, **kw: seen.append((op, payload)) or {"streams": []})
    c = TestClient(main.app)
    c.seen = seen
    return c


def test_spec_endpoint(client):
    r = client.post("/spec", json={})
    assert r.status_code == 200
    assert r.json()["secret_fields"] == ["password", "ssl_mode.client_key", "tunnel_method.ssh_key"]
    assert r.json()["connection_specification"]["required"] == ["host", "port", "username"]


def test_check_with_missing_host_is_400_without_values(client):
    r = client.post("/check", json={"config": {"port": 5432, "username": "u", "password": "S3CRET"}})
    assert r.status_code == 400
    err = r.json()["error"]
    assert err["category"] == "config" and {"path": "host", "reason": "is required"} in err["details"]
    assert "S3CRET" not in r.text and client.seen == []  # the connector was never started


def test_discover_and_reads_validate_too_and_free_the_slot(client):
    bad = {"config": {"host": "h", "port": "x", "username": "u"}}
    assert client.post("/discover", json=bad).status_code == 400
    assert client.post("/reads", json={**bad, "streams": ["a"]}).status_code == 400
    assert main._slots_used == 0
    ok = client.post("/discover", json={"config": GOOD})
    assert ok.status_code == 200
    op, payload = client.seen[-1]
    assert op == "discover" and payload["secret_fields"] == ["password", "ssl_mode.client_key", "tunnel_method.ssh_key"]


def test_unknown_connector_is_rejected(client):
    r = client.post("/spec", json={"connector": "source-other"})
    assert r.status_code == 400 and r.json()["error"]["category"] == "config"
