"""
tests/test_persistence.py  — Week 3

Tests verify:
  1. db_service functions call the right Django ORM methods
  2. TCPServer writes to DB on SET/DELETE
  3. TCPServer restores data from DB on startup (_load_from_db)
  4. All Week 2 tests still pass (use_db=False skips MySQL)

MySQL is fully mocked — no real DB needed to run tests.
"""
import threading
import socket
import time
import pytest
from unittest.mock import patch, MagicMock, call

import os
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
try:
    django.setup()
except Exception:
    pass


# ──────────────────────────────────────────────
# TEST HELPERS (same pattern as Week 2)
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
            result = save_user("shubham", "hashed_pw")
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
                defaults={"cache_value": "gurugram"}
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
            assert "alice" in srv._stores
            assert "bob" in srv._stores

    def test_restores_cache_entries_on_startup(self):
        """Each user's cache entries should be loaded into their LRUCache."""
        users_data = [{"username": "alice", "password_hash": "h"}]
        entries = {"name": "alice", "city": "delhi"}

        with patch("apps.users.db_service.load_all_users", return_value=users_data), \
             patch("apps.users.db_service.get_all_entries", return_value=entries):
            from server.tcp_server import TCPServer
            srv = TCPServer(host="127.0.0.1", port=0, use_db=True)
            srv._load_from_db()
            store = srv._stores["alice"]
            assert store.cache.get("name") == "alice"
            assert store.cache.get("city") == "delhi"

    def test_empty_db_loads_fine(self):
        """Server should start fine even if no users exist in DB."""
        with patch("apps.users.db_service.load_all_users", return_value=[]), \
             patch("apps.users.db_service.get_all_entries", return_value={}):
            from server.tcp_server import TCPServer
            srv = TCPServer(host="127.0.0.1", port=0, use_db=True)
            srv._load_from_db()
            assert len(srv._stores) == 0


# ──────────────────────────────────────────────
# REGRESSION: ALL WEEK 2 TESTS STILL PASS
# ──────────────────────────────────────────────

class TestWeek2Regression:
    """All Week 2 behavior must still work with use_db=False."""

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