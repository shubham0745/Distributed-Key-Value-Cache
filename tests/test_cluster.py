"""
tests/test_cluster.py — Week 5 end to end

Three real TCP servers (RAM only, fast Raft timings) and the CLI client:
redirects to the leader, replication to every node, surviving the
leader's death, and the one-shot command line.
"""
import socket
import threading
import time
from unittest.mock import patch

import pytest

from client.cli import (CacheClient, AuthError, main as cli_main, parse_args,
                        authenticate, ask_mode, ask_password)
from server.tcp_server import TCPServer

FAST = {"heartbeat_interval": 0.05, "election_timeout": (0.3, 0.6)}


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(condition, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return condition()


class RawSession:
    """Log in with a plain socket — no redirects, no retries."""

    def __init__(self, address: str, username: str, password: str):
        host, port = address.rsplit(":", 1)
        self.sock = socket.create_connection((host, int(port)), timeout=5)
        self.reader = self.sock.makefile("rb")
        self.reader.readline()
        for line in ("LOGIN", username, password):
            reply = self.ask(line)
        if not reply.startswith("READY"):
            self.close()
            raise AssertionError(reply)

    def ask(self, line: str) -> str:
        self.sock.sendall((line + "\n").encode())
        return self.reader.readline().decode().strip()

    def close(self):
        self.reader.close()
        self.sock.close()


class Cluster:

    def __init__(self, size: int = 3, stale_reads: bool = False):
        client_ports = [get_free_port() for _ in range(size)]
        raft_ports = [get_free_port() for _ in range(size)]
        ids = [f"node{i + 1}" for i in range(size)]
        self.addresses = {ids[i]: f"127.0.0.1:{client_ports[i]}" for i in range(size)}
        self.servers = [
            TCPServer("127.0.0.1", client_ports[i], use_db=False,
                      node_id=ids[i], raft_port=raft_ports[i],
                      peers=[f"127.0.0.1:{p}" for j, p in enumerate(raft_ports) if j != i],
                      client_addresses=self.addresses, stale_reads=stale_reads,
                      raft_options=FAST)
            for i in range(size)
        ]

    def start(self):
        for srv in self.servers:
            threading.Thread(target=srv.start, daemon=True).start()
        assert wait_until(lambda: self.leader() is not None), "no leader elected"

    def stop(self):
        for srv in self.servers:
            srv.stop()

    def leader(self):
        leaders = [s for s in self.servers if s._running and s.raft.is_leader()]
        return leaders[0] if len(leaders) == 1 else None

    def address_of(self, srv) -> str:
        return self.addresses[srv.node_id]

    def followers(self):
        leader = self.leader()
        return [s for s in self.servers if s is not leader]


@pytest.fixture
def fast_hashing():
    with patch("django.contrib.auth.hashers.make_password", side_effect=lambda p: f"hashed_{p}"), \
         patch("django.contrib.auth.hashers.check_password",
               side_effect=lambda plain, hashed: hashed == f"hashed_{plain}"):
        yield


@pytest.fixture
def cluster(fast_hashing):
    c = Cluster()
    c.start()
    yield c
    c.stop()


@pytest.fixture
def stale_cluster(fast_hashing):
    c = Cluster(stale_reads=True)
    c.start()
    yield c
    c.stop()


class TestRedirects:

    def test_follower_answers_writes_with_leader_address(self, cluster):
        follower = cluster.followers()[0]
        host, port = cluster.address_of(follower).rsplit(":", 1)
        sock = socket.create_connection((host, int(port)), timeout=5)
        reader = sock.makefile("rb")
        reader.readline()
        sock.sendall(b"SIGNUP\n")
        reply = reader.readline().decode().strip()
        sock.close()
        assert reply == f"ERROR: NOT_LEADER {cluster.address_of(cluster.leader())}"

    def test_client_follows_redirect_to_leader(self, cluster):
        follower = cluster.followers()[0]
        with CacheClient([cluster.address_of(follower)]) as client:
            client.signup("shubham", "pass1234")
            client.set("city", "gurugram")
            assert client.address == cluster.address_of(cluster.leader())
            assert client.get("city") == "gurugram"

    def test_wrong_password_raises_auth_error(self, cluster):
        with CacheClient(list(cluster.addresses.values())) as client:
            client.signup("rahul", "pass1234")
        with CacheClient(list(cluster.addresses.values())) as client:
            with pytest.raises(AuthError):
                client.login("rahul", "nope")


class TestReplication:

    def test_every_node_gets_the_data(self, cluster):
        with CacheClient(list(cluster.addresses.values())) as client:
            client.signup("alice", "pass1234")
            client.set("lang", "python")
            client.set("editor", "vs code")

        for srv in cluster.servers:
            assert wait_until(lambda: srv.state_machine.get("alice", "editor") == "vs code"), \
                f"{srv.node_id} never caught up"

    def test_follower_redirects_reads_by_default(self, cluster):
        with CacheClient(list(cluster.addresses.values())) as client:
            client.signup("reader", "pass1234")
        follower = cluster.followers()[0]
        assert wait_until(lambda: follower.state_machine.user_exists("reader"))
        session = RawSession(cluster.address_of(follower), "reader", "pass1234")
        reply = session.ask("GET anything")
        session.close()
        assert reply == f"ERROR: NOT_LEADER {cluster.address_of(cluster.leader())}"

    def test_follower_defers_unknown_login_to_leader(self, cluster):
        follower = cluster.followers()[0]
        host, port = cluster.address_of(follower).rsplit(":", 1)
        sock = socket.create_connection((host, int(port)), timeout=5)
        reader = sock.makefile("rb")
        reader.readline()
        sock.sendall(b"LOGIN\nnobody_yet\n")
        reader.readline()                           # "Username:"
        reply = reader.readline().decode().strip()
        sock.close()
        assert reply == f"ERROR: NOT_LEADER {cluster.address_of(cluster.leader())}"

    def test_unknown_user_still_rejected_by_leader(self, cluster):
        with CacheClient(list(cluster.addresses.values())) as client:
            with pytest.raises(AuthError, match="not found"):
                client.login("ghost", "pass1234")

    def test_read_right_after_write_from_a_new_client(self, cluster):
        nodes = list(cluster.addresses.values())
        with CacheClient(nodes) as writer:
            writer.signup("ryw", "pass1234")
            writer.set("fresh", "value")
        with CacheClient(nodes) as reader:         # new session, may start on a follower
            reader.login("ryw", "pass1234")
            assert reader.get("fresh") == "value"

    def test_stale_reads_mode_serves_reads_on_followers(self, stale_cluster):
        with CacheClient(list(stale_cluster.addresses.values())) as client:
            client.signup("carol", "pass1234")
            client.set("editor", "vs code")

        for srv in stale_cluster.servers:
            def served_locally():
                try:
                    session = RawSession(stale_cluster.address_of(srv), "carol", "pass1234")
                except AssertionError:
                    return False                  # SIGNUP not applied here yet
                try:
                    return session.ask("GET editor") == "vs code"
                finally:
                    session.close()
            assert wait_until(served_locally), f"{srv.node_id} never served the value"

    def test_exactly_one_leader_reported_by_info(self, cluster):
        with CacheClient(list(cluster.addresses.values())) as client:
            client.signup("infouser", "pass1234")
        roles = []
        for srv in cluster.servers:
            assert wait_until(lambda: srv.state_machine.user_exists("infouser"))
            session = RawSession(cluster.address_of(srv), "infouser", "pass1234")
            roles.append(session.ask("INFO").split("role=")[1].split()[0])
            session.close()
        assert sorted(roles) == ["follower", "follower", "leader"]


class TestFailover:

    def test_client_keeps_working_after_leader_dies(self, cluster):
        with CacheClient(list(cluster.addresses.values())) as client:
            client.signup("bob", "pass1234")
            client.set("before", "1")
            old_leader = cluster.leader()
            # make sure the followers have the data before killing the leader
            assert wait_until(lambda: all(
                s.state_machine.get("bob", "before") == "1" for s in cluster.followers()))

            old_leader.stop()
            client.set("after", "2")               # redirected to the new leader

            assert client.address != cluster.address_of(old_leader)
            assert client.get("before") == "1"
            assert client.get("after") == "2"


def feed(monkeypatch, answers, passwords):
    """Script the interactive prompts: input() answers and hidden passwords."""
    answers, passwords = iter(answers), iter(passwords)
    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    monkeypatch.setattr("client.cli.getpass.getpass", lambda prompt="": next(passwords))


class TestInteractiveLogin:

    def test_unrecognised_choice_is_asked_again(self, monkeypatch, capsys):
        feed(monkeypatch, ["1234", "signup"], [])
        assert ask_mode() == "signup"
        assert "Please type LOGIN or SIGNUP" in capsys.readouterr().out

    def test_signup_password_checked_locally_and_confirmed(self, monkeypatch, capsys):
        feed(monkeypatch, [], ["", "12", "abcd", "abce", "abcd", "abcd"])
        assert ask_password("signup") == "abcd"
        out = capsys.readouterr().out
        assert "at least 4 characters" in out
        assert "don't match" in out

    def test_mistyped_session_still_signs_up(self, cluster, monkeypatch):
        # choice "1234" → re-asked; empty password → re-asked; then it works
        feed(monkeypatch, ["1234", "signup", "newuser"], ["", "pass1234", "pass1234"])
        with CacheClient(list(cluster.addresses.values())) as client:
            assert authenticate(client, parse_args([])) is True
            client.set("k", "v")
            assert client.get("k") == "v"

    def test_wrong_password_only_asks_for_the_password_again(self, cluster, monkeypatch):
        with CacheClient(list(cluster.addresses.values())) as client:
            client.signup("sam", "pass1234")
        feed(monkeypatch, ["login", "sam"], ["wrong", "pass1234"])
        with CacheClient(list(cluster.addresses.values())) as client:
            assert authenticate(client, parse_args([])) is True


class TestCommandLine:

    def test_one_shot_command(self, cluster, capsys):
        nodes = ",".join(cluster.addresses.values())
        code = cli_main(["--nodes", nodes, "--user", "cliuser", "--password", "pass1234",
                         "--signup", "SET", "greeting", "hello world"])
        assert code == 0
        code = cli_main(["--nodes", nodes, "--user", "cliuser", "--password", "pass1234",
                         "GET", "greeting"])
        assert code == 0
        assert capsys.readouterr().out.strip().endswith("hello world")

    def test_bad_password_exits_non_zero(self, cluster, capsys):
        nodes = ",".join(cluster.addresses.values())
        cli_main(["--nodes", nodes, "--user", "cli2", "--password", "pass1234", "--signup", "INFO"])
        code = cli_main(["--nodes", nodes, "--user", "cli2", "--password", "wrong", "INFO"])
        assert code == 1
