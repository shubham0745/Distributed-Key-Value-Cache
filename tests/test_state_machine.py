"""
tests/test_state_machine.py — what Raft replicates

apply() must be deterministic: the same entries in the same order give
the same data on every node, whatever each node's clock or RAM cache
says. Every test runs in RAM-only mode and in database mode (SQLite).
"""
import io
from unittest.mock import patch

import pytest

import server.state_machine as sm_module
from raft import LogEntry
from server.state_machine import CacheStateMachine

FUTURE = 99_999_999_999_999          # an expiry that never passes
NOW = 1_700_000_000_000              # "now" stamped into commands


@pytest.fixture(params=["ram", "db"])
def machine(request):
    if request.param == "db":
        request.getfixturevalue("clean_db")
    sm = CacheStateMachine(use_db=request.param == "db")
    sm.apply(LogEntry(1, 1, "SIGNUP hashed_pw", "zoe"))
    return sm


def apply(sm, index, command, request_id=""):
    return sm.apply(LogEntry(1, index, command, "zoe", request_id=request_id))


class TestDeterministicExpiry:

    def test_expiry_decisions_use_the_time_in_the_command(self, machine):
        apply(machine, 2, f"SETEX k {NOW + 1000} v")
        # at NOW the key is alive, at NOW+2000 it has expired — whatever
        # the real clock says
        assert apply(machine, 3, f"EXPIREAT k {NOW + 5000} {NOW}") is True
        assert apply(machine, 4, f"EXPIREAT k {NOW + 9000} {NOW + 6000}") is False
        assert apply(machine, 5, f"DELETE k {NOW + 6000}") is False

    def test_sweep_removes_exactly_the_expired_keys(self, machine):
        apply(machine, 2, f"SETEX old {NOW - 1} gone")
        apply(machine, 3, f"SETEX new {FUTURE} stays")
        apply(machine, 4, "SET plain stays")
        apply(machine, 5, f"SWEEP {NOW}")
        store = machine.get_store("zoe")
        assert not store.cache.has("old")
        assert machine.get("zoe", "new") == "stays"
        assert machine.get("zoe", "plain") == "stays"
        if machine.use_db:
            from apps.users.db_service import get_entry
            assert get_entry("zoe", "old") is None

    def test_reads_hide_expired_keys_before_the_sweep(self, machine):
        apply(machine, 2, f"SETEX k {NOW} v")
        assert machine.get("zoe", "k") is None           # real clock is past NOW
        assert machine.ttl("zoe", "k") == -2
        assert machine.keys("zoe") == []

    def test_persist_and_ttl(self, machine):
        apply(machine, 2, f"SETEX k {FUTURE} v")
        assert machine.ttl("zoe", "k") > 0
        assert apply(machine, 3, f"PERSIST k {NOW}") is True
        assert machine.ttl("zoe", "k") == -1
        assert apply(machine, 4, f"PERSIST k {NOW}") is False
        assert machine.ttl("zoe", "missing") == -2

    def test_set_clears_expiry(self, machine):
        apply(machine, 2, f"SETEX k {FUTURE} v1")
        apply(machine, 3, "SET k v2")
        assert machine.ttl("zoe", "k") == -1

    def test_keys_pattern(self, machine):
        for i, key in enumerate(["a:1", "a:2", "b:1"], start=2):
            apply(machine, i, f"SET {key} x")
        assert machine.keys("zoe") == ["a:1", "a:2", "b:1"]
        assert machine.keys("zoe", "a:*") == ["a:1", "a:2"]

    def test_next_expiry(self, machine):
        assert machine.next_expiry() is None
        apply(machine, 2, f"SETEX a {NOW + 50} x")
        apply(machine, 3, f"SETEX b {NOW + 10} x")
        assert machine.next_expiry() == NOW + 10


class TestSessions:

    def test_same_request_id_applies_once(self, machine):
        apply(machine, 2, "SET k v")
        assert apply(machine, 3, "DELETE k", "c1:1") is True
        apply(machine, 4, "SET k again")                        # someone re-creates k
        assert apply(machine, 5, "DELETE k", "c1:1") is True    # the retry: cached answer...
        assert machine.get("zoe", "k") == "again"               # ...and NOT run again
        assert apply(machine, 6, "DELETE k", "c1:2") is True    # a new request runs

    def test_older_duplicate_is_ignored(self, machine):
        apply(machine, 2, "SET k 1", "c1:5")
        assert apply(machine, 3, "SET k 2", "c1:4") is None
        assert machine.get("zoe", "k") == "1"

    def test_eviction_is_deterministic(self, machine, monkeypatch):
        monkeypatch.setattr(sm_module, "MAX_SESSIONS", 2)
        apply(machine, 2, "SET a 1", "first:1")
        apply(machine, 3, "SET b 1", "second:1")
        apply(machine, 4, "SET c 1", "third:1")          # evicts "first" (oldest index)
        assert set(machine.sessions) == {"second", "third"}
        if machine.use_db:
            from apps.users.db_service import load_sessions
            assert {row[0] for row in load_sessions()} == {"second", "third"}


class TestSnapshots:

    def test_round_trip_keeps_users_keys_expiry_and_sessions(self, machine):
        apply(machine, 2, "SET plain value with spaces")
        apply(machine, 3, f"SETEX temp {FUTURE} soon", "c9:1")
        out = io.StringIO()
        machine.snapshot(out)

        fresh = CacheStateMachine(use_db=machine.use_db)
        if machine.use_db:
            from apps.users.db_service import replace_from
            replace_from([])
        fresh.restore(io.StringIO(out.getvalue()))
        assert fresh.get("zoe", "plain") == "value with spaces"
        assert fresh.ttl("zoe", "temp") > 0
        assert fresh.session_result("c9:1") is True
        with patch("django.contrib.auth.hashers.check_password",
                   side_effect=lambda p, h: h == f"hashed_{p}"):
            assert fresh.verify_password("zoe", "pw")


class TestDatabaseMode:

    def test_expiry_and_sessions_survive_a_restart(self, clean_db):
        sm = CacheStateMachine(use_db=True)
        sm.apply(LogEntry(1, 1, "SIGNUP hashed_pw", "zoe"))
        sm.apply(LogEntry(1, 2, f"SETEX k {FUTURE} v", "zoe", request_id="c1:1"))

        restarted = CacheStateMachine(use_db=True)
        restarted.load_from_db()
        assert restarted.ttl("zoe", "k") > 0
        assert restarted.session_result("c1:1") is True

    def test_evicted_keys_still_listed_and_expired(self, clean_db):
        sm = CacheStateMachine(use_db=True, capacity=2)
        sm.apply(LogEntry(1, 1, "SIGNUP hashed_pw", "zoe"))
        sm.apply(LogEntry(1, 2, f"SETEX a {NOW} x", "zoe"))       # already expired
        sm.apply(LogEntry(1, 3, "SET b x", "zoe"))
        sm.apply(LogEntry(1, 4, "SET c x", "zoe"))                # evicts 'a' from RAM
        assert sm.keys("zoe") == ["b", "c"]                       # DB knows 'a' expired
        assert sm.get("zoe", "a") is None
        sm.apply(LogEntry(1, 5, f"SWEEP {NOW}", ""))
        from apps.users.db_service import get_entry
        assert get_entry("zoe", "a") is None
