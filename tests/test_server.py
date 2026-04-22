"""
Week 2 Tests — TCP Server + Store + Auth
Run with: pytest tests/test_server.py -v
"""
import threading
import socket
import time
import pytest
from unittest.mock import patch

import os
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
try:
    django.setup()
except Exception:
    pass


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]

def make_server(port: int):
    from server.tcp_server import TCPServer
    return TCPServer(host="127.0.0.1", port=port)

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
    return sock.recv(4096).decode().strip()

def signup_and_auth(port: int, username: str, password: str) -> socket.socket:
    sock = connect_client(port)
    sr(sock, "SIGNUP")
    sr(sock, username)
    sr(sock, password)
    return sock

MOCK_MAKE = patch("django.contrib.auth.hashers.make_password", side_effect=lambda p: f"hashed_{p}")
MOCK_CHECK = patch("django.contrib.auth.hashers.check_password", side_effect=lambda plain, hashed: hashed == f"hashed_{plain}")


class TestStore:

    def test_store_has_cache(self):
        from store.store import Store
        assert Store("user", "hash").cache is not None

    def test_store_cache_operations(self):
        from store.store import Store
        s = Store("user", "hash")
        s.cache.set("k", "v")
        assert s.cache.get("k") == "v"

    def test_store_username(self):
        from store.store import Store
        assert Store("shubham", "hash").username == "shubham"

    def test_two_stores_are_isolated(self):
        from store.store import Store
        a = Store("alice", "ha")
        b = Store("bob", "hb")
        a.cache.set("name", "alice_val")
        b.cache.set("name", "bob_val")
        assert a.cache.get("name") == "alice_val"
        assert b.cache.get("name") == "bob_val"

    def test_password_verification_correct(self):
        from store.store import Store
        with patch("django.contrib.auth.hashers.check_password", return_value=True):
            assert Store("user", "h").verify_password("correct") is True

    def test_password_verification_wrong(self):
        from store.store import Store
        with patch("django.contrib.auth.hashers.check_password", return_value=False):
            assert Store("user", "h").verify_password("wrong") is False


class TestTCPServerAuth:

    @pytest.fixture()
    def srv(self):
        port = get_free_port()
        with MOCK_MAKE, MOCK_CHECK:
            server = make_server(port)
            start_server(server)
            yield server, port
            server.stop()

    def test_signup_new_user(self, srv):
        _, port = srv
        sock = connect_client(port)
        resp = sr(sock, "SIGNUP")
        assert "username" in resp.lower() or "choose" in resp.lower()
        sr(sock, "newuser")
        resp = sr(sock, "password123")
        assert "ready" in resp.lower()
        sock.close()

    def test_login_after_signup(self, srv):
        _, port = srv
        s1 = connect_client(port)
        sr(s1, "SIGNUP"); sr(s1, "logintest"); sr(s1, "mypassword")
        s1.close(); time.sleep(0.1)
        s2 = connect_client(port)
        sr(s2, "LOGIN"); sr(s2, "logintest")
        resp = sr(s2, "mypassword")
        assert "ready" in resp.lower()
        s2.close()

    def test_login_wrong_password(self, srv):
        _, port = srv
        s1 = connect_client(port)
        sr(s1, "SIGNUP"); sr(s1, "wpuser"); sr(s1, "correctpass")
        s1.close(); time.sleep(0.1)
        s2 = connect_client(port)
        sr(s2, "LOGIN"); sr(s2, "wpuser")
        resp = sr(s2, "wrongpass")
        assert "error" in resp.lower() or "wrong" in resp.lower()
        s2.close()

    def test_login_nonexistent_user(self, srv):
        _, port = srv
        sock = connect_client(port)
        sr(sock, "LOGIN")
        resp = sr(sock, "ghost_user_xyz")
        assert "error" in resp.lower() or "not found" in resp.lower()
        sock.close()

    def test_duplicate_signup(self, srv):
        _, port = srv
        s1 = connect_client(port)
        sr(s1, "SIGNUP"); sr(s1, "dupuser"); sr(s1, "pass1234")
        s1.close(); time.sleep(0.1)
        s2 = connect_client(port)
        sr(s2, "SIGNUP")
        resp = sr(s2, "dupuser")
        assert "taken" in resp.lower() or "error" in resp.lower()
        s2.close()

    def test_invalid_auth_command(self, srv):
        _, port = srv
        sock = connect_client(port)
        resp = sr(sock, "HELLO")
        assert "error" in resp.lower()
        sock.close()


class TestTCPServerCommands:

    @pytest.fixture()
    def session(self):
        port = get_free_port()
        with MOCK_MAKE, MOCK_CHECK:
            server = make_server(port)
            start_server(server)
            sock = signup_and_auth(port, "cmduser", "cmdpass")
            yield sock
            sock.close()
            server.stop()

    def test_set_and_get(self, session):
        assert sr(session, "SET city gurugram") == "OK"
        assert sr(session, "GET city") == "gurugram"

    def test_get_missing_key(self, session):
        assert sr(session, "GET nonexistent") == "NULL"

    def test_has_existing_key(self, session):
        sr(session, "SET language python")
        assert sr(session, "HAS language") == "1"

    def test_has_missing_key(self, session):
        assert sr(session, "HAS ghost") == "0"

    def test_delete_existing_key(self, session):
        sr(session, "SET tmp val")
        assert sr(session, "DELETE tmp") == "OK"
        assert sr(session, "GET tmp") == "NULL"

    def test_delete_missing_key(self, session):
        assert sr(session, "DELETE nobody") == "NULL"

    def test_overwrite_value(self, session):
        sr(session, "SET counter 1")
        sr(session, "SET counter 2")
        assert sr(session, "GET counter") == "2"

    def test_unknown_command(self, session):
        assert "error" in sr(session, "FLUSH all").lower()

    def test_set_missing_value(self, session):
        assert "error" in sr(session, "SET onlykey").lower()


class TestUserIsolation:

    def test_two_users_cannot_see_each_others_data(self):
        port = get_free_port()
        with MOCK_MAKE, MOCK_CHECK:
            server = make_server(port)
            start_server(server)
            alice = signup_and_auth(port, "alice", "pass1234")
            sr(alice, "SET secret alice_only")
            bob = signup_and_auth(port, "bob", "pass5678")
            resp = sr(bob, "GET secret")
            assert resp == "NULL", f"Bob saw Alice's data! Got: {resp}"
            alice.close()
            bob.close()
            server.stop()