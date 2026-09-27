"""
tests/test_persistence.py — MySQL persistence

Tests verify:
  1. db_service functions call the right Django ORM methods
  2. TCPServer writes to DB on SET/DELETE
  3. TCPServer restores data from DB on startup (_load_from_db)
  4. The same code against a real database (TestRealDatabase)
  5. RAM-only mode still works (use_db=False skips MySQL)

Most tests mock the ORM; TestRealDatabase uses the throwaway SQLite
database set up in conftest.py.
"""
import threading
import socket
import time
import pytest
from unittest.mock import patch, MagicMock

from tests.ports import get_free_port

import os
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
try:
    django.setup()
except Exception:
    pass


# ──────────────────────────────────────────────
# TEST HELPERS
# ──────────────────────────────────────────────


def make_server(port: int, use_db: bool = False):
    from server.tcp_server import TCPServer
    return TCPServer(host="127.0.0.1", port=port, use_db=use_db)


def start_server(srv):
    t = threading.Thread(target=srv.start, daemon=True)
    t.start()
    time.sleep(0.3)
    return t


def connect_client(port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect(("127.0.0.1", port))
    sock.recv(4096)
    return sock


def sr(sock: socket.socket, msg: str) -> str:
    sock.sendall((msg + "\n").encode())
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


def signup_and_auth(port: int, username: str, password: str) -> socket.socket:
    sock = connect_client(port)
    sr(sock, "SIGNUP")
    sr(sock, username)
    sr(sock, password)
    return sock


MOCK_MAKE = patch(
    "django.contrib.auth.hashers.make_password",
    side_effect=lambda p: f"hashed_{p}"
)
MOCK_CHECK = patch(
    "django.contrib.auth.hashers.check_password",
    side_effect=lambda plain, hashed: hashed == f"hashed_{plain}"
)


# ──────────────────────────────────────────────
# DB SERVICE UNIT TESTS (ORM fully mocked)
# ──────────────────────────────────────────────

class TestDbService:
    """
    Test db_service functions in isolation.
    We mock Django's ORM so no real DB is needed.
    """

    def test_save_user_calls_create(self):
        mock_user = MagicMock()
        with patch("apps.users.models.CacheUser.objects") as mock_obj:
            mock_obj.create.return_value = mock_user
            from apps.users.db_service import save_user
            save_user("shubham", "hashed_pw")
            mock_obj.create.assert_called_once_with(
                username="shubham",
                password_hash="hashed_pw"
            )

    def test_user_exists_true(self):
        with patch("apps.users.models.CacheUser.objects") as mock_obj:
            mock_obj.filter.return_value.exists.return_value = True
            from apps.users.db_service import user_exists
            assert user_exists("shubham") is True

    def test_user_exists_false(self):
        with patch("apps.users.models.CacheUser.objects") as mock_obj:
            mock_obj.filter.return_value.exists.return_value = False
            from apps.users.db_service import user_exists
            assert user_exists("nobody") is False

    def test_get_user_found(self):
        mock_user = MagicMock()
        mock_user.username = "shubham"
        with patch("apps.users.models.CacheUser.objects") as mock_obj:
            mock_obj.get.return_value = mock_user
            from apps.users.db_service import get_user
            result = get_user("shubham")
            assert result.username == "shubham"

    def test_get_user_not_found(self):
        from apps.users.models import CacheUser
        with patch("apps.users.models.CacheUser.objects") as mock_obj:
            mock_obj.get.side_effect = CacheUser.DoesNotExist
            from apps.users.db_service import get_user
            result = get_user("ghost")
            assert result is None

    def test_save_entry_calls_update_or_create(self):
        mock_user = MagicMock()
        with patch("apps.users.models.CacheUser.objects") as mock_cu, \
             patch("apps.users.models.CacheEntry.objects") as mock_ce:
            mock_cu.get.return_value = mock_user
            from apps.users.db_service import save_entry
            save_entry("shubham", "city", "gurugram")
            mock_ce.update_or_create.assert_called_once_with(
                user=mock_user,
                cache_key="city",
                defaults={"cache_value": "gurugram", "expire_at": None}  # SET clears expiry
            )

    def test_delete_entry_returns_true_when_deleted(self):
        mock_user = MagicMock()
        with patch("apps.users.models.CacheUser.objects") as mock_cu, \
             patch("apps.users.models.CacheEntry.objects") as mock_ce:
            mock_cu.get.return_value = mock_user
            mock_ce.filter.return_value.delete.return_value = (1, {})
            from apps.users.db_service import delete_entry
            assert delete_entry("shubham", "city") is True

    def test_delete_entry_returns_false_when_not_found(self):
        mock_user = MagicMock()
        with patch("apps.users.models.CacheUser.objects") as mock_cu, \
             patch("apps.users.models.CacheEntry.objects") as mock_ce:
            mock_cu.get.return_value = mock_user
            mock_ce.filter.return_value.delete.return_value = (0, {})
            from apps.users.db_service import delete_entry
            assert delete_entry("shubham", "ghost") is False

    def test_get_all_entries_returns_dict(self):
        mock_user = MagicMock()
        entry1 = MagicMock(cache_key="name", cache_value="shubham")
        entry2 = MagicMock(cache_key="city", cache_value="gurugram")
        with patch("apps.users.models.CacheUser.objects") as mock_cu, \
             patch("apps.users.models.CacheEntry.objects") as mock_ce:
            mock_cu.get.return_value = mock_user
            mock_ce.filter.return_value = [entry1, entry2]
            from apps.users.db_service import get_all_entries
            result = get_all_entries("shubham")
            assert result == {"name": "shubham", "city": "gurugram"}

    def test_load_all_users_returns_list(self):
        u1 = MagicMock(username="alice", password_hash="h_alice")
        u2 = MagicMock(username="bob", password_hash="h_bob")
        with patch("apps.users.models.CacheUser.objects") as mock_obj:
            mock_obj.all.return_value = [u1, u2]
            from apps.users.db_service import load_all_users
            result = load_all_users()
            assert len(result) == 2
            assert result[0]["username"] == "alice"
            assert result[1]["username"] == "bob"


# ──────────────────────────────────────────────
# SERVER + DB INTEGRATION TESTS
# ──────────────────────────────────────────────

class TestServerWritesToDB:
    """
    Verify the server calls DB functions on SET/DELETE.
    Server runs with use_db=True but DB calls are mocked.
    """

    @pytest.fixture()
    def db_server(self):
        port = get_free_port()
        with MOCK_MAKE, MOCK_CHECK, \
             patch("apps.users.db_service.save_user") as mock_su, \
             patch("apps.users.db_service.save_entry") as mock_se, \
             patch("apps.users.db_service.delete_entry") as mock_de, \
             patch("apps.users.db_service.user_exists", return_value=False), \
             patch("apps.users.db_service.load_all_users", return_value=[]):
            from server.tcp_server import TCPServer
            srv = TCPServer(host="127.0.0.1", port=port, use_db=True)
            t = threading.Thread(target=srv.start, daemon=True)
            t.start()
            time.sleep(0.3)
            yield srv, port, mock_su, mock_se, mock_de
            srv.stop()

    def test_signup_writes_user_to_db(self, db_server):
        srv, port, mock_su, mock_se, mock_de = db_server
        sock = connect_client(port)
        sr(sock, "SIGNUP")
        sr(sock, "newuser")
        sr(sock, "pass1234")
        sock.close()
        time.sleep(0.1)
        mock_su.assert_called_once_with("newuser", "hashed_pass1234")

    def test_set_writes_entry_to_db(self, db_server):
        srv, port, mock_su, mock_se, mock_de = db_server
        sock = signup_and_auth(port, "writer", "pass1234")
        sr(sock, "SET city gurugram")
        sock.close()
        time.sleep(0.1)
        mock_se.assert_called_with("writer", "city", "gurugram")

    def test_delete_removes_entry_from_db(self, db_server):
        srv, port, mock_su, mock_se, mock_de = db_server
        sock = signup_and_auth(port, "deleter", "pass1234")
        sr(sock, "SET temp val")
        sr(sock, "DELETE temp")
        sock.close()
        time.sleep(0.1)
        mock_de.assert_called_with("deleter", "temp")


class TestServerRestoresFromDB:
    """
    Verify _load_from_db correctly restores users and their cache entries.
    """

    def test_restores_users_on_startup(self):
        """Server should load users from MySQL and make them available."""
        users_data = [
            {"username": "alice", "password_hash": "hashed_pass1"},
            {"username": "bob", "password_hash": "hashed_pass2"},
        ]
        with patch("apps.users.db_service.load_all_users", return_value=users_data), \
             patch("apps.users.db_service.get_all_entries", return_value={}):
            from server.tcp_server import TCPServer
            srv = TCPServer(host="127.0.0.1", port=0, use_db=True)
            srv._load_from_db()
            assert "alice" in srv.state_machine.stores
            assert "bob" in srv.state_machine.stores

    def test_restores_cache_entries_on_startup(self):
        """Each user's cache entries should be loaded into their LRUCache."""
        users_data = [{"username": "alice", "password_hash": "h"}]
        entries = {"name": "alice", "city": "delhi"}

        with patch("apps.users.db_service.load_all_users", return_value=users_data), \
             patch("apps.users.db_service.get_all_entries", return_value=entries):
            from server.tcp_server import TCPServer
            srv = TCPServer(host="127.0.0.1", port=0, use_db=True)
            srv._load_from_db()
            store = srv.state_machine.stores["alice"]
            assert store.cache.get("name") == "alice"
            assert store.cache.get("city") == "delhi"

    def test_empty_db_loads_fine(self):
        """Server should start fine even if no users exist in DB."""
        with patch("apps.users.db_service.load_all_users", return_value=[]), \
             patch("apps.users.db_service.get_all_entries", return_value={}):
            from server.tcp_server import TCPServer
            srv = TCPServer(host="127.0.0.1", port=0, use_db=True)
            srv._load_from_db()
            assert len(srv.state_machine.stores) == 0


# ──────────────────────────────────────────────
# REAL DATABASE (a throwaway SQLite file — see conftest.py)
# ──────────────────────────────────────────────

class TestRealDatabase:
    """
    No ORM mocks here: rows really get written and read back, so these
    cover restarts, LRU eviction + DB fallback, and the startup checks.
    """

    @pytest.fixture(autouse=True)
    def _db(self, clean_db):
        with MOCK_MAKE, MOCK_CHECK:
            self.servers = []
            yield
            for srv in self.servers:
                srv.stop()

    def boot(self, capacity: int = 1000):
        port = get_free_port()
        srv = make_server(port, use_db=True)
        srv.state_machine.capacity = capacity
        start_server(srv)
        self.servers.append(srv)
        return srv, port

    def login(self, port, username, password):
        sock = connect_client(port)
        sr(sock, "LOGIN")
        sr(sock, username)
        assert sr(sock, password) == f"READY:{username}"
        return sock

    def test_data_survives_restart(self):
        srv, port = self.boot()
        sock = signup_and_auth(port, "alice", "pass1234")
        assert sr(sock, "SET city delhi") == "OK"
        sock.close()
        srv.stop()

        _, port2 = self.boot()
        sock = self.login(port2, "alice", "pass1234")
        assert sr(sock, "GET city") == "delhi"
        sock.close()

    def test_evicted_key_is_read_back_from_db(self):
        _, port = self.boot(capacity=2)
        sock = signup_and_auth(port, "carol", "pass1234")
        for key in "abc":
            sr(sock, f"SET {key} value_{key}")       # 'a' falls out of RAM
        assert sr(sock, "GET a") == "value_a"
        assert sr(sock, "HAS a") == "1"
        sock.close()

    def test_deleting_an_evicted_key_removes_it_from_db(self):
        srv, port = self.boot(capacity=2)
        sock = signup_and_auth(port, "erin", "pass1234")
        for key in "abc":
            sr(sock, f"SET {key} value_{key}")
        assert sr(sock, "DELETE a") == "OK"
        sock.close()
        srv.stop()

        _, port2 = self.boot()
        sock = self.login(port2, "erin", "pass1234")
        assert sr(sock, "GET a") == "NULL"            # didn't come back to life
        sock.close()

    def test_user_added_to_db_after_startup_can_log_in(self):
        _, port = self.boot()
        from apps.users.db_service import save_user, save_entry
        save_user("bob", "hashed_pw1234")
        save_entry("bob", "color", "blue")

        sock = self.login(port, "bob", "pw1234")
        assert sr(sock, "GET color") == "blue"
        assert sr(sock, "SET size large") == "OK"
        sock.close()

    def test_startup_fails_loudly_when_db_unreadable(self):
        from server.state_machine import StateLoadError
        srv = make_server(get_free_port(), use_db=True)
        with patch("apps.users.db_service.load_all_users",
                   side_effect=RuntimeError("MySQL is down")):
            with pytest.raises(StateLoadError):
                srv.start()

    def test_snapshot_round_trip(self):
        import io
        from raft import LogEntry
        from server.state_machine import CacheStateMachine
        sm = CacheStateMachine(use_db=True)
        sm.apply(LogEntry(1, 1, "SIGNUP hashed_x", "zoe"))
        sm.apply(LogEntry(1, 2, "SET lang python", "zoe", request_id="c1:1"))
        sm.apply(LogEntry(1, 3, "SETEX tmp 99999999999999 soon", "zoe"))
        snapshot = io.StringIO()
        sm.snapshot(snapshot)

        from apps.users.db_service import replace_from
        replace_from([])                             # wipe
        fresh = CacheStateMachine(use_db=True)
        fresh.restore(io.StringIO(snapshot.getvalue()))
        assert fresh.get("zoe", "lang") == "python"
        assert fresh.user_exists("zoe")
        assert fresh.ttl("zoe", "tmp") > 0
        assert fresh.session_result("c1:1") is True

    def test_cluster_node_keeps_raft_state_in_db(self):
        from server.tcp_server import TCPServer
        from apps.cluster.raft_storage import DjangoRaftStorage
        srv = TCPServer(port=get_free_port(), use_db=True, node_id="node1",
                        raft_port=get_free_port(), peers=["127.0.0.1:1"])
        assert isinstance(srv.raft._storage, DjangoRaftStorage)


# ──────────────────────────────────────────────
# RAM-ONLY MODE (use_db=False)
# ──────────────────────────────────────────────

class TestRamOnlyMode:
    """Everything still works without a database (use_db=False)."""

    @pytest.fixture()
    def session(self):
        port = get_free_port()
        with MOCK_MAKE, MOCK_CHECK:
            server = make_server(port, use_db=False)
            start_server(server)
            sock = signup_and_auth(port, "cmduser", "cmdpass")
            yield sock
            sock.close()
            server.stop()

    def test_set_and_get(self, session):
        assert sr(session, "SET name shubham") == "OK"
        assert sr(session, "GET name") == "shubham"

    def test_get_missing(self, session):
        assert sr(session, "GET ghost") == "NULL"

    def test_has_true(self, session):
        sr(session, "SET k v")
        assert sr(session, "HAS k") == "1"

    def test_has_false(self, session):
        assert sr(session, "HAS missing") == "0"

    def test_delete(self, session):
        sr(session, "SET d v")
        assert sr(session, "DELETE d") == "OK"
        assert sr(session, "GET d") == "NULL"

    def test_overwrite(self, session):
        sr(session, "SET x 1")
        sr(session, "SET x 2")
        assert sr(session, "GET x") == "2"