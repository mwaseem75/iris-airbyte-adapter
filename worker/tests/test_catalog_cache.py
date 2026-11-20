import json
import os
import time

import pytest

from app import catalog_cache


@pytest.fixture(autouse=True)
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog_cache, "CACHE_DIR", tmp_path)
    monkeypatch.setenv("CATALOG_CACHE_TTL_SECONDS", "3600")


CONFIG = {"host": "db", "password": "s3cret-value"}


def test_fingerprint_is_stable_and_depends_on_config_and_version():
    a = catalog_cache.fingerprint("source-postgres", "3.8.11", CONFIG)
    assert a == catalog_cache.fingerprint("source-postgres", "3.8.11", {"password": "s3cret-value", "host": "db"})
    assert a != catalog_cache.fingerprint("source-postgres", "3.8.11", {**CONFIG, "host": "other"})
    assert a != catalog_cache.fingerprint("source-postgres", "3.9.0", CONFIG)


def test_the_config_and_its_secrets_are_never_written_to_disk(tmp_path):
    key = catalog_cache.fingerprint("source-postgres", "3.8.11", CONFIG)
    catalog_cache.save(key, {"streams": [{"name": "t"}]})
    for f in tmp_path.iterdir():
        text = f.read_text()
        assert "s3cret-value" not in text and "host" not in text.replace("hostile", "")


def test_save_then_load():
    key = catalog_cache.fingerprint("c", "1", CONFIG)
    assert catalog_cache.load(key) is None
    catalog_cache.save(key, {"streams": [{"name": "t"}]})
    assert catalog_cache.load(key) == {"streams": [{"name": "t"}]}


def test_entry_older_than_ttl_is_ignored(tmp_path, monkeypatch):
    key = catalog_cache.fingerprint("c", "1", CONFIG)
    catalog_cache.save(key, {"streams": []})
    old = time.time() - 7200
    os.utime(tmp_path / f"{key}.json", (old, old))
    assert catalog_cache.load(key) is None
    monkeypatch.setenv("CATALOG_CACHE_TTL_SECONDS", "10000")
    assert catalog_cache.load(key) == {"streams": []}


def test_ttl_zero_turns_the_cache_off(monkeypatch, tmp_path):
    monkeypatch.setenv("CATALOG_CACHE_TTL_SECONDS", "0")
    key = catalog_cache.fingerprint("c", "1", CONFIG)
    catalog_cache.save(key, {"streams": []})
    assert catalog_cache.load(key) is None
    assert not (tmp_path / f"{key}.json").exists()
