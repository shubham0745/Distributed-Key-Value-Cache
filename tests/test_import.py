"""
tests/test_import.py — scripts/import_data.py: copy an old single-node
database into a running cluster through the admin session.
"""
import sqlite3
import time

from client.cli import CacheClient
from scripts import import_data
from tests.test_cluster import ADMIN_TOKEN, cluster, fast_hashing, wait_until  # noqa: F401

FUTURE = int(time.time() * 1000) + 3_600_000
PAST = int(time.time() * 1000) - 1000


def make_source(path, with_expiry: bool = True):
    """An old database with the same tables the server uses."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE cache_users (id INTEGER PRIMARY KEY, username TEXT, "
                 "password_hash TEXT, created_at TEXT)")
    conn.execute("CREATE TABLE cache_entries (id INTEGER PRIMARY KEY, user_id INTEGER, "
                 "cache_key TEXT, cache_value TEXT, updated_at TEXT"
                 + (", expire_at INTEGER" if with_expiry else "") + ")")
    conn.executemany("INSERT INTO cache_users (id, username, password_hash) VALUES (?, ?, ?)",
                     [(1, "old_alice", "hashed_alice_pw"), (2, "old_bob", "hashed_bob_pw")])
    rows = [(1, "city", "delhi", None), (1, "note", "hello world", None),
            (2, "temp", "soon", FUTURE), (2, "gone", "old", PAST)]
    if with_expiry:
        conn.executemany("INSERT INTO cache_entries (user_id, cache_key, cache_value, expire_at) "
                         "VALUES (?, ?, ?, ?)", rows)
    else:
        conn.executemany("INSERT INTO cache_entries (user_id, cache_key, cache_value) "
                         "VALUES (?, ?, ?)", [r[:3] for r in rows])
    conn.commit()
    return conn


def run_import(cluster, conn):
    with CacheClient(cluster.addresses()) as admin:
        admin.admin_login(ADMIN_TOKEN)
        return import_data.import_into(admin, import_data.read_users(conn),
                                       import_data.read_entries(conn), report=lambda *_: None)


class TestImport:

    def test_users_keys_and_expiry_arrive_on_every_node(self, cluster, tmp_path):
        conn = make_source(tmp_path / "old.sqlite3")
        stats = run_import(cluster, conn)
        assert stats == {"users_created": 2, "users_existing": 0, "keys": 3, "keys_expired": 1}

        with CacheClient(cluster.addresses()) as client:
            client.login("old_alice", "alice_pw")          # same password as before
            assert client.get("city") == "delhi"
            assert client.get("note") == "hello world"
        with CacheClient(cluster.addresses()) as client:
            client.login("old_bob", "bob_pw")
            assert client.ttl("temp") > 3000
            assert client.get("gone") is None
        for srv in cluster.servers:
            assert wait_until(lambda: srv.state_machine.get("old_alice", "city") == "delhi")

    def test_running_it_twice_is_harmless(self, cluster, tmp_path):
        conn = make_source(tmp_path / "old.sqlite3")
        run_import(cluster, conn)
        stats = run_import(cluster, conn)
        assert stats["users_created"] == 0
        assert stats["users_existing"] == 2

    def test_source_from_before_expiry_support(self, cluster, tmp_path):
        conn = make_source(tmp_path / "older.sqlite3", with_expiry=False)
        stats = run_import(cluster, conn)
        assert stats["keys"] == 4

    def test_refuses_to_run_without_a_token(self, tmp_path, monkeypatch, capsys):
        monkeypatch.delenv("CACHE_ADMIN_TOKEN", raising=False)
        make_source(tmp_path / "old.sqlite3").close()
        code = import_data.main(["--source-sqlite", str(tmp_path / "old.sqlite3"),
                                 "--cluster", str(tmp_path / "missing.json")])
        assert code == 1
        assert "admin token" in capsys.readouterr().out
