"""
tests/test_raft.py — Week 4: Leader Election

Tests verify:
  1. A single node becomes leader (no competition)
  2. In a 3-node cluster, exactly ONE leader is elected
  3. Leader sends heartbeats that prevent new elections
  4. When leader dies, a new leader is elected
  5. Nodes reject stale terms
  6. Vote is only granted once per term
"""
import time
import threading
import pytest
from unittest.mock import MagicMock

from raft.types import RaftState, RaftNode, LogEntry
from raft.rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
)
from raft.node import RaftEngine, ELECTION_TIMEOUT_MAX
import socket


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_engine(peers=None, on_leader=None, on_follower=None) -> tuple[RaftEngine, int]:
    """Create a RaftEngine on a random free port."""
    port = get_free_port()
    node_id = f"node_{port}"
    engine = RaftEngine(
        node_id=node_id,
        host="127.0.0.1",
        port=port,
        peers=peers or [],
        on_become_leader=on_leader,
        on_become_follower=on_follower,
    )
    return engine, port


def wait_for_state(engine: RaftEngine, state: RaftState,
                   timeout: float = 5.0) -> bool:
    """Poll until engine reaches the expected state or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if engine.get_state() == state:
            return True
        time.sleep(0.05)
    return False


# ──────────────────────────────────────────────
# UNIT TESTS — RaftNode data type
# ──────────────────────────────────────────────

class TestRaftNode:

    def test_initial_state_is_follower(self):
        node = RaftNode(node_id="n1", peers=[])
        assert node.state == RaftState.FOLLOWER

    def test_initial_term_is_zero(self):
        node = RaftNode(node_id="n1", peers=[])
        assert node.current_term == 0

    def test_initial_voted_for_is_none(self):
        node = RaftNode(node_id="n1", peers=[])
        assert node.voted_for is None

    def test_last_log_index_empty(self):
        node = RaftNode(node_id="n1", peers=[])
        assert node.last_log_index() == 0

    def test_last_log_term_empty(self):
        node = RaftNode(node_id="n1", peers=[])
        assert node.last_log_term() == 0

    def test_last_log_index_with_entries(self):
        node = RaftNode(node_id="n1", peers=[])
        node.log.append(LogEntry(term=1, index=1, command="SET k v", username="u"))
        node.log.append(LogEntry(term=1, index=2, command="SET k2 v2", username="u"))
        assert node.last_log_index() == 2

    def test_last_log_term_with_entries(self):
        node = RaftNode(node_id="n1", peers=[])
        node.log.append(LogEntry(term=3, index=1, command="SET k v", username="u"))
        assert node.last_log_term() == 3

    def test_is_leader(self):
        node = RaftNode(node_id="n1", peers=[])
        assert node.is_leader() is False
        node.state = RaftState.LEADER
        assert node.is_leader() is True

    def test_is_follower(self):
        node = RaftNode(node_id="n1", peers=[])
        assert node.is_follower() is True

    def test_is_candidate(self):
        node = RaftNode(node_id="n1", peers=[])
        assert node.is_candidate() is False
        node.state = RaftState.CANDIDATE
        assert node.is_candidate() is True


# ──────────────────────────────────────────────
# UNIT TESTS — Vote handler logic
# ──────────────────────────────────────────────

class TestRequestVoteHandler:
    """Test _handle_request_vote directly without network."""

    def setup_method(self):
        self.engine, self.port = make_engine()

    def test_grants_vote_to_valid_candidate(self):
        req = RequestVoteRequest(
            term=1,
            candidate_id="node_other",
            last_log_index=0,
            last_log_term=0,
        )
        resp = self.engine._handle_request_vote(req)
        assert resp.vote_granted is True
        assert resp.term == 1

    def test_rejects_vote_for_stale_term(self):
        # Set our term higher first
        with self.engine._state_lock:
            self.engine._node.current_term = 5
        req = RequestVoteRequest(
            term=3,  # lower than ours
            candidate_id="node_other",
            last_log_index=0,
            last_log_term=0,
        )
        resp = self.engine._handle_request_vote(req)
        assert resp.vote_granted is False

    def test_votes_only_once_per_term(self):
        req1 = RequestVoteRequest(term=1, candidate_id="node_a",
                                  last_log_index=0, last_log_term=0)
        req2 = RequestVoteRequest(term=1, candidate_id="node_b",
                                  last_log_index=0, last_log_term=0)
        resp1 = self.engine._handle_request_vote(req1)
        resp2 = self.engine._handle_request_vote(req2)
        assert resp1.vote_granted is True
        assert resp2.vote_granted is False  # already voted for node_a

    def test_updates_term_on_higher_term_request(self):
        req = RequestVoteRequest(term=10, candidate_id="node_other",
                                 last_log_index=0, last_log_term=0)
        self.engine._handle_request_vote(req)
        assert self.engine.get_term() == 10

    def test_rejects_candidate_with_stale_log(self):
        # Give ourselves a longer log
        with self.engine._state_lock:
            self.engine._node.log.append(
                LogEntry(term=2, index=1, command="SET k v", username="u")
            )
            self.engine._node.current_term = 2
        req = RequestVoteRequest(
            term=2,
            candidate_id="node_other",
            last_log_index=0,   # candidate has empty log
            last_log_term=0,
        )
        resp = self.engine._handle_request_vote(req)
        assert resp.vote_granted is False


# ──────────────────────────────────────────────
# UNIT TESTS — AppendEntries handler
# ──────────────────────────────────────────────

class TestAppendEntriesHandler:

    def setup_method(self):
        self.engine, self.port = make_engine()

    def test_accepts_valid_heartbeat(self):
        req = AppendEntriesRequest(
            term=1, leader_id="leader",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0,
        )
        resp = self.engine._handle_append_entries(req)
        assert resp.success is True

    def test_rejects_stale_leader(self):
        with self.engine._state_lock:
            self.engine._node.current_term = 5
        req = AppendEntriesRequest(
            term=3, leader_id="old_leader",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0,
        )
        resp = self.engine._handle_append_entries(req)
        assert resp.success is False

    def test_becomes_follower_on_valid_heartbeat(self):
        # Start as candidate
        with self.engine._state_lock:
            self.engine._node.state = RaftState.CANDIDATE
            self.engine._node.current_term = 1
        req = AppendEntriesRequest(
            term=1, leader_id="leader",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0,
        )
        self.engine._handle_append_entries(req)
        assert self.engine.get_state() == RaftState.FOLLOWER

    def test_updates_term_on_higher_term_heartbeat(self):
        req = AppendEntriesRequest(
            term=7, leader_id="leader",
            prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0,
        )
        self.engine._handle_append_entries(req)
        assert self.engine.get_term() == 7


# ──────────────────────────────────────────────
# INTEGRATION TESTS — Real election over network
# ──────────────────────────────────────────────

class TestLeaderElection:
    """Full election tests with real TCP connections between nodes."""

    def test_single_node_becomes_leader(self):
        """A node with no peers should elect itself immediately."""
        engine, _ = make_engine(peers=[])
        engine.start()
        # Single node — no need to get votes from anyone
        became_leader = wait_for_state(engine, RaftState.LEADER, timeout=5.0)
        engine.stop()
        assert became_leader, "Single node should become leader"

    def test_three_node_cluster_elects_one_leader(self):
        """In a 3-node cluster, exactly ONE node becomes leader."""
        p1, p2, p3 = get_free_port(), get_free_port(), get_free_port()
        peers_1 = [f"127.0.0.1:{p2}", f"127.0.0.1:{p3}"]
        peers_2 = [f"127.0.0.1:{p1}", f"127.0.0.1:{p3}"]
        peers_3 = [f"127.0.0.1:{p1}", f"127.0.0.1:{p2}"]

        e1 = RaftEngine("node1", "127.0.0.1", p1, peers_1)
        e2 = RaftEngine("node2", "127.0.0.1", p2, peers_2)
        e3 = RaftEngine("node3", "127.0.0.1", p3, peers_3)

        e1.start(); e2.start(); e3.start()

        # Wait for election to complete
        time.sleep(ELECTION_TIMEOUT_MAX + 1.5)

        states = [e1.get_state(), e2.get_state(), e3.get_state()]
        leaders   = states.count(RaftState.LEADER)
        followers = states.count(RaftState.FOLLOWER)

        e1.stop(); e2.stop(); e3.stop()

        assert leaders == 1,   f"Expected 1 leader, got {leaders}. States: {states}"
        assert followers == 2, f"Expected 2 followers, got {followers}"

    def test_leader_callback_fires(self):
        """on_become_leader callback should be called when node wins."""
        callback = MagicMock()
        engine, _ = make_engine(peers=[], on_leader=callback)
        engine.start()
        wait_for_state(engine, RaftState.LEADER, timeout=5.0)
        engine.stop()
        callback.assert_called_once()

    def test_new_leader_elected_after_leader_dies(self):
        """If the leader stops, remaining nodes elect a new leader."""
        p1, p2, p3 = get_free_port(), get_free_port(), get_free_port()
        e1 = RaftEngine("node1", "127.0.0.1", p1,
                        [f"127.0.0.1:{p2}", f"127.0.0.1:{p3}"])
        e2 = RaftEngine("node2", "127.0.0.1", p2,
                        [f"127.0.0.1:{p1}", f"127.0.0.1:{p3}"])
        e3 = RaftEngine("node3", "127.0.0.1", p3,
                        [f"127.0.0.1:{p1}", f"127.0.0.1:{p2}"])

        e1.start(); e2.start(); e3.start()
        time.sleep(ELECTION_TIMEOUT_MAX + 1.5)

        # Find and kill the leader
        engines = [e1, e2, e3]
        leader = next((e for e in engines if e.is_leader()), None)
        assert leader is not None, "No leader elected initially"
        leader.stop()

        survivors = [e for e in engines if e is not leader]
        time.sleep(ELECTION_TIMEOUT_MAX + 1.5)

        new_leaders = [e for e in survivors if e.is_leader()]
        for e in survivors:
            e.stop()

        assert len(new_leaders) == 1, \
            f"Expected 1 new leader after old leader died, got {len(new_leaders)}"