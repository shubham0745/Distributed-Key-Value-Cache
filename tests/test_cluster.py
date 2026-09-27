"""
tests/test_cluster.py — end to end on real TCP servers

Three (sometimes four) servers in RAM-only mode with fast Raft timings,
driven through plain sockets and through the CLI client:
  - any node serves any command (writes forwarded, reads linearizable)
  - replication, failover, graceful leadership hand-over
  - membership changes through the admin session
  - key expiry, exactly-once retries, the interactive and one-shot CLI
"""
import socket
import threading
import time
from unittest.mock import patch

import pytest

from client.cli import (CacheClient, AuthError, ClientError, main as cli_main, parse_args,
                        authenticate, ask_mode, ask_password)
from raft import Member
from server.tcp_server import TCPServer

FAST = {"heartbeat_interval": 0.05, "election_timeout": (0.4, 0.8)}
ADMIN_TOKEN = "test-admin-token"


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


class Raw:
    """A plain socket session — exactly what telnet would do."""

    def __init__(self, address: str):
        host, port = address.rsplit(":", 1)
        self.sock = socket.create_connection((host, int(port)), timeout=10)
        self.reader = self.sock.makefile("rb")
        self.reader.readline()                      # welcome banner

    def ask(self, line: str) -> str:
        self.sock.sendall((line + "\n").encode())
        return self.reader.readline().decode().strip()

    def login(self, username: str, password: str) -> str:
        self.ask("LOGIN")
        self.ask(username)
        return self.ask(password)

    def close(self):
        self.reader.close()
        self.sock.close()


class Cluster:

    def __init__(self, size: int = 3, stale_reads: bool = False):
        self.stale_reads = stale_reads
        self.specs = [(f"node{i + 1}", get_free_port(), get_free_port()) for i in range(size)]
        self.members = [Member(nid, f"127.0.0.1:{rp}", f"127.0.0.1:{cp}")
                        for nid, cp, rp in self.specs]
        self.servers = [self._make(nid, cp, rp) for nid, cp, rp in self.specs]

    def _make(self, node_id, client_port, raft_port, join=False) -> TCPServer:
        return TCPServer("127.0.0.1", client_port, use_db=False, node_id=node_id,
                         raft_port=raft_port, members=self.members, join=join,
                         stale_reads=self.stale_reads, admin_token=ADMIN_TOKEN,
                         raft_options=FAST)

    def start(self):
        for srv in self.servers:
            self._run(srv)
        assert wait_until(lambda: self.leader() is not None), "no leader elected"

    @staticmethod
    def _run(srv):
        threading.Thread(target=srv.start, daemon=True).start()
        assert wait_until(lambda: srv._running)

    def add_joining_server(self, node_id: str) -> TCPServer:
        """Start a node with --join semantics (no membership yet)."""
        cp, rp = get_free_port(), get_free_port()
        srv = self._make(node_id, cp, rp, join=True)
        self.servers.append(srv)
        self._run(srv)
        return srv

    def stop(self):
        for srv in self.servers:
            srv.stop()

    def leader(self):
        leaders = [s for s in self.servers if s._running and s.raft.is_leader()]
        return leaders[0] if len(leaders) == 1 else None

    def followers(self):
        leader = self.leader()
        return [s for s in self.servers if s is not leader and s._running]

    @staticmethod
    def address_of(srv) -> str:
        return f"{srv.host}:{srv.port}"

    def addresses(self) -> list[str]:
        return [self.address_of(s) for s in self.servers if s._running]

    def client(self, **kwargs) -> CacheClient:
        return CacheClient(self.addresses(), **kwargs)


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


# ──────────────────────────────────────────────
# ANY NODE SERVES ANY COMMAND
# ──────────────────────────────────────────────

class TestAnyNode:

    def test_follower_accepts_writes_by_forwarding(self, cluster):
        follower = Raw(cluster.address_of(cluster.followers()[0]))
        assert follower.ask("SIGNUP") == "Choose username:"
        follower.ask("teluser")
        assert follower.ask("pass1234") == "READY:teluser"
        assert follower.ask("SET city gurugram") == "OK"
        assert follower.ask("GET city") == "gurugram"
        follower.close()

    def test_client_stays_on_the_follower_it_picked(self, cluster):
        address = cluster.address_of(cluster.followers()[0])
        with CacheClient([address]) as client:
            client.signup("shubham", "pass1234")
            client.set("city", "gurugram")
            assert client.address == address          # no redirect needed
            assert client.get("city") == "gurugram"

    def test_follower_reads_are_linearizable(self, cluster):
        """A read on a follower right after a write elsewhere sees that write."""
        with cluster.client() as writer:
            writer.signup("alice", "pass1234")
        followers = [Raw(cluster.address_of(s)) for s in cluster.followers()]
        for session in followers:
            assert session.login("alice", "pass1234") == "READY:alice"
        leader = Raw(cluster.address_of(cluster.leader()))
        leader.login("alice", "pass1234")
        for i in range(20):
            assert leader.ask(f"SET counter {i}") == "OK"
            for session in followers:
                assert session.ask("GET counter") == str(i)
        for session in followers + [leader]:
            session.close()

    def test_login_on_follower_right_after_signup(self, cluster):
        leader_address = cluster.address_of(cluster.leader())
        follower_address = cluster.address_of(cluster.followers()[0])
        with CacheClient([leader_address]) as client:
            client.signup("fresh", "pass1234")
        session = Raw(follower_address)
        assert session.login("fresh", "pass1234") == "READY:fresh"
        session.close()

    def test_unknown_user_is_rejected(self, cluster):
        with cluster.client() as client:
            with pytest.raises(AuthError, match="not found"):
                client.login("ghost", "pass1234")

    def test_wrong_password_raises_auth_error(self, cluster):
        with cluster.client() as client:
            client.signup("rahul", "pass1234")
        with cluster.client() as client:
            with pytest.raises(AuthError):
                client.login("rahul", "nope")

    def test_stale_reads_mode_serves_reads_locally(self, stale_cluster):
        with stale_cluster.client() as client:
            client.signup("carol", "pass1234")
            client.set("editor", "vs code")
        for srv in stale_cluster.servers:
            assert wait_until(lambda: srv.state_machine.get("carol", "editor") == "vs code")


# ──────────────────────────────────────────────
# REPLICATION + FAILOVER
# ──────────────────────────────────────────────

class TestReplication:

    def test_every_node_gets_the_data(self, cluster):
        with cluster.client() as client:
            client.signup("alice", "pass1234")
            client.set("lang", "python")
            client.set("editor", "vs code")
        for srv in cluster.servers:
            assert wait_until(lambda: srv.state_machine.get("alice", "editor") == "vs code"), \
                f"{srv.node_id} never caught up"

    def test_exactly_one_leader_reported_by_info(self, cluster):
        with cluster.client() as client:
            client.signup("infouser", "pass1234")
        roles = []
        for srv in cluster.servers:
            session = Raw(cluster.address_of(srv))
            assert session.login("infouser", "pass1234").startswith("READY")
            roles.append(session.ask("INFO").split("role=")[1].split()[0])
            session.close()
        assert sorted(roles) == ["follower", "follower", "leader"]


class TestFailover:

    def test_client_keeps_working_after_leader_crashes(self, cluster):
        with cluster.client() as client:
            client.signup("bob", "pass1234")
            client.set("before", "1")
            old_leader = cluster.leader()
            old_leader.stop()                          # crash, no hand-over
            client.set("after", "2")
            assert client.address != cluster.address_of(old_leader)
            assert client.get("before") == "1"
            assert client.get("after") == "2"

    def test_graceful_stop_hands_leadership_over_immediately(self, cluster):
        with cluster.client() as client:
            client.signup("hand", "pass1234")
            client.set("k", "v")
            old_leader = cluster.leader()
            term = old_leader.raft.get_term()
            started = time.time()
            old_leader.stop(graceful=True)
            assert wait_until(lambda: cluster.leader() is not None, timeout=1.0)
            took = time.time() - started
            new_leader = cluster.leader()
            assert new_leader is not old_leader
            assert new_leader.raft.get_term() == term + 1   # one clean election
            assert took < 2.0                              # no election timeout wait
            assert client.get("k") == "v"


# ──────────────────────────────────────────────
# EXPIRY + KEYS
# ──────────────────────────────────────────────

class TestExpiry:

    def test_setex_ttl_expire_persist(self, cluster):
        with cluster.client() as client:
            client.signup("ttluser", "pass1234")
            client.setex("session", 100, "abc")
            assert 99 <= client.ttl("session") <= 100
            client.set("plain", "x")
            assert client.ttl("plain") == -1
            assert client.ttl("missing") == -2
            assert client.expire("plain", 50) is True
            assert 49 <= client.ttl("plain") <= 50
            assert client.persist("plain") is True
            assert client.ttl("plain") == -1
            assert client.persist("plain") is False     # nothing to remove
            assert client.expire("missing", 5) is False

    def test_expired_key_disappears_on_every_node(self, cluster):
        with cluster.client() as client:
            client.signup("shortlived", "pass1234")
            client.setex("temp", 1, "bye")
            client.set("keep", "me")
            assert client.get("temp") == "bye"
            assert wait_until(lambda: client.get("temp") is None, timeout=4)
            assert client.has("temp") is False
            assert client.get("keep") == "me"
        # The leader's SWEEP removes it from every node's memory
        for srv in cluster.servers:
            assert wait_until(lambda: not srv.state_machine.get_store("shortlived").cache.has("temp"),
                              timeout=4), srv.node_id

    def test_set_clears_an_expiry(self, cluster):
        with cluster.client() as client:
            client.signup("resetter", "pass1234")
            client.setex("k", 100, "v1")
            client.set("k", "v2")
            assert client.ttl("k") == -1

    def test_keys_with_pattern(self, cluster):
        with cluster.client() as client:
            client.signup("keyuser", "pass1234")
            for key in ("user:1", "user:2", "order:9"):
                client.set(key, "x")
            client.setex("user:tmp", 1, "x")
            assert client.keys() == ["order:9", "user:1", "user:2", "user:tmp"]
            assert client.keys("user:?") == ["user:1", "user:2"]
            assert wait_until(lambda: "user:tmp" not in client.keys(), timeout=4)

    def test_bad_ttl_values_rejected(self, cluster):
        with cluster.client() as client:
            client.signup("badttl", "pass1234")
            for command in ("SETEX k 0 v", "SETEX k -5 v", "SETEX k abc v", "EXPIRE k 0"):
                assert client.execute(command).startswith("ERROR")


# ──────────────────────────────────────────────
# EXACTLY-ONCE WRITES
# ──────────────────────────────────────────────

class TestExactlyOnce:

    def test_retried_request_is_applied_once(self, cluster):
        session = Raw(cluster.address_of(cluster.followers()[0]))
        session.ask("SIGNUP"); session.ask("once"); session.ask("pass1234")
        assert session.ask("SET k v") == "OK"
        assert session.ask("@cli42:1 DELETE k") == "OK"
        # Same request id again (a retry after a lost reply): same answer,
        # not "NULL" — the delete was not run a second time
        assert session.ask("@cli42:1 DELETE k") == "OK"
        assert session.ask("@cli42:2 DELETE k") == "NULL"   # a NEW request
        session.close()

    def test_retried_signup_logs_in_instead_of_failing(self, cluster):
        first = Raw(cluster.address_of(cluster.leader()))
        assert first.ask("@signup77:1 SIGNUP") == "Choose username:"
        first.ask("retry_user")
        assert first.ask("pass1234") == "READY:retry_user"
        first.close()
        retry = Raw(cluster.address_of(cluster.followers()[0]))
        assert retry.ask("@signup77:1 SIGNUP") == "Choose username:"
        assert retry.ask("retry_user") == "Choose password:"
        assert retry.ask("pass1234") == "READY:retry_user"
        retry.close()

    def test_bad_tag_is_rejected(self, cluster):
        session = Raw(cluster.address_of(cluster.leader()))
        assert session.ask("@bad tag LOGIN").startswith("ERROR: Bad request tag")
        session.close()


# ──────────────────────────────────────────────
# MEMBERSHIP (admin session)
# ──────────────────────────────────────────────

class TestMembership:

    def admin(self, cluster) -> CacheClient:
        client = cluster.client()
        client.admin_login(ADMIN_TOKEN)
        return client

    def test_members_lists_every_node_and_the_leader(self, cluster):
        with self.admin(cluster) as admin:
            members = admin.members()
        assert members["leader"] == cluster.leader().node_id
        assert sorted(k for k in members if k != "leader") == ["node1", "node2", "node3"]

    def test_add_a_node_to_a_running_cluster(self, cluster):
        with cluster.client() as client:
            client.signup("grow", "pass1234")
            client.set("before", "1")

        node4 = cluster.add_joining_server("node4")
        time.sleep(1.0)
        assert node4.raft.members() == []              # waits to be added
        assert node4.raft.get_term() == 0              # and doesn't campaign

        with self.admin(cluster) as admin:
            admin.add_node("node4", cluster.address_of(node4), node4.raft.raft_address)
            assert "node4" in admin.members()

        for srv in cluster.servers:
            assert wait_until(lambda: len(srv.raft.members()) == 4), srv.node_id
        assert wait_until(lambda: node4.state_machine.get("grow", "before") == "1")
        with CacheClient([cluster.address_of(node4)]) as client:
            client.login("grow", "pass1234")
            client.set("after", "2")
            assert client.get("before") == "1"

    def test_remove_a_follower(self, cluster):
        victim = cluster.followers()[0]
        with self.admin(cluster) as admin:
            admin.remove_node(victim.node_id)
            assert victim.node_id not in admin.members()
        victim.stop()
        with cluster.client() as client:              # 2 of 2 still a majority
            client.signup("after_removal", "pass1234")
            client.set("k", "v")

    def test_remove_the_leader(self, cluster):
        old_leader = cluster.leader()
        with self.admin(cluster) as admin:
            admin.remove_node(old_leader.node_id)
        assert wait_until(lambda: not old_leader.raft.is_leader())
        assert wait_until(lambda: cluster.leader() not in (None, old_leader))
        assert old_leader.node_id not in [m.node_id for m in cluster.leader().raft.members()]
        old_leader.stop()
        with cluster.client() as client:
            client.signup("new_era", "pass1234")
            client.set("k", "v")

    def test_adding_an_existing_member_is_a_no_op(self, cluster):
        node = cluster.servers[0]
        with self.admin(cluster) as admin:
            admin.add_node(node.node_id, cluster.address_of(node), node.raft.raft_address)
            with pytest.raises(ClientError, match="different addresses"):
                admin.add_node(node.node_id, "127.0.0.1:1", "127.0.0.1:2")

    def test_cannot_remove_unknown_node(self, cluster):
        with self.admin(cluster) as admin:
            with pytest.raises(ClientError, match="not a member"):
                admin.remove_node("nodeX")

    def test_admin_needs_the_right_token(self, cluster):
        with cluster.client() as client:
            with pytest.raises(AuthError, match="Wrong token"):
                client.admin_login("guess")

    def test_admin_disabled_without_token(self, fast_hashing):
        srv = TCPServer("127.0.0.1", get_free_port(), use_db=False)
        threading.Thread(target=srv.start, daemon=True).start()
        assert wait_until(lambda: srv._running)
        try:
            with CacheClient([f"127.0.0.1:{srv.port}"]) as client:
                with pytest.raises(AuthError, match="disabled"):
                    client.admin_login("anything")
        finally:
            srv.stop()


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

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
        with cluster.client() as client:
            assert authenticate(client, parse_args([])) is True
            client.set("k", "v")
            assert client.get("k") == "v"

    def test_wrong_password_only_asks_for_the_password_again(self, cluster, monkeypatch):
        with cluster.client() as client:
            client.signup("sam", "pass1234")
        feed(monkeypatch, ["login", "sam"], ["wrong", "pass1234"])
        with cluster.client() as client:
            assert authenticate(client, parse_args([])) is True


class TestCommandLine:

    def test_one_shot_command(self, cluster, capsys):
        nodes = ",".join(cluster.addresses())
        code = cli_main(["--nodes", nodes, "--user", "cliuser", "--password", "pass1234",
                         "--signup", "SET", "greeting", "hello world"])
        assert code == 0
        code = cli_main(["--nodes", nodes, "--user", "cliuser", "--password", "pass1234",
                         "GET", "greeting"])
        assert code == 0
        assert capsys.readouterr().out.strip().endswith("hello world")

    def test_bad_password_exits_non_zero(self, cluster, capsys):
        nodes = ",".join(cluster.addresses())
        cli_main(["--nodes", nodes, "--user", "cli2", "--password", "pass1234", "--signup", "INFO"])
        code = cli_main(["--nodes", nodes, "--user", "cli2", "--password", "wrong", "INFO"])
        assert code == 1

    def test_admin_one_shot(self, cluster, capsys):
        nodes = ",".join(cluster.addresses())
        code = cli_main(["--nodes", nodes, "--admin", "--admin-token", ADMIN_TOKEN, "MEMBERS"])
        assert code == 0
        assert "leader=" in capsys.readouterr().out
