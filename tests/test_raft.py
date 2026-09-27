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


# ══════════════════════════════════════════════
# WEEK 5 — LOG REPLICATION
# ══════════════════════════════════════════════

from dataclasses import asdict

from raft import MemoryRaftStorage, NotLeaderError, ProposalError

FAST = {"heartbeat_interval": 0.05, "election_timeout": (0.3, 0.6)}


def entry(index: int, term: int = 1, command: str = None) -> LogEntry:
    return LogEntry(term=term, index=index,
                    command=command or f"SET k{index} v{index}", username="u")


def wait_until(condition, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


class KVMachine:
    """Tiny state machine for engine tests: SET k v / DELETE k."""

    def __init__(self):
        self.data = {}
        self.applied = []
        self._lock = threading.Lock()

    def apply(self, e: LogEntry):
        with self._lock:
            self.applied.append(e.command)
            verb, _, rest = e.command.partition(" ")
            if verb == "SET":
                key, _, value = rest.partition(" ")
                self.data[key] = value
                return True
            if verb == "DELETE":
                return self.data.pop(rest, None) is not None

    def snapshot(self):
        with self._lock:
            return dict(self.data)

    def restore(self, data):
        with self._lock:
            self.data = dict(data or {})


class RaftCluster:
    """N in-process engines talking over real sockets, with fast timings."""

    def __init__(self, size: int = 3, **options):
        self.ports = [get_free_port() for _ in range(size)]
        self.options = {**FAST, **options}
        self.storages = [MemoryRaftStorage() for _ in range(size)]
        self.machines = [KVMachine() for _ in range(size)]
        self.engines = [self._make(i) for i in range(size)]

    def _make(self, i: int) -> RaftEngine:
        peers = [f"127.0.0.1:{p}" for j, p in enumerate(self.ports) if j != i]
        m = self.machines[i]
        return RaftEngine(f"node{i + 1}", "127.0.0.1", self.ports[i], peers,
                          apply_fn=m.apply, snapshot_fn=m.snapshot, restore_fn=m.restore,
                          storage=self.storages[i], **self.options)

    def start(self, *indexes):
        for i in indexes or range(len(self.engines)):
            self.engines[i].start()

    def restart(self, i: int):
        """Crash + restart: same storage (term/vote/log), empty state machine."""
        self.engines[i].stop()
        self.machines[i] = KVMachine()
        self.engines[i] = self._make(i)
        self.engines[i].start()

    def stop_all(self):
        for e in self.engines:
            e.stop()

    def leader(self, among=None, timeout: float = 5.0) -> RaftEngine:
        candidates = [self.engines[i] for i in (among or range(len(self.engines)))]
        found = []

        def elected():
            found[:] = [e for e in candidates if e.is_leader()]
            return bool(found)

        assert wait_until(elected, timeout), "no leader elected"
        return found[0]

    def machine_of(self, engine: RaftEngine) -> KVMachine:
        return self.machines[self.engines.index(engine)]


class TestAppendEntriesReplication:
    """_handle_append_entries with real entries — no network."""

    def setup_method(self):
        self.engine, _ = make_engine()

    def append(self, entries=(), term=1, prev_index=0, prev_term=0, commit=0):
        return self.engine._handle_append_entries(AppendEntriesRequest(
            term=term, leader_id="leader", prev_log_index=prev_index,
            prev_log_term=prev_term, entries=list(entries), leader_commit=commit))

    def test_appends_new_entries(self):
        resp = self.append([entry(1), entry(2)])
        assert resp.success is True
        assert resp.match_index == 2
        assert self.engine._node.last_log_index() == 2

    def test_accepts_entries_as_json_dicts(self):
        resp = self.append([asdict(entry(1))])
        assert resp.success is True
        assert self.engine._node.entry_at(1).command == "SET k1 v1"

    def test_rejects_when_previous_entry_missing(self):
        resp = self.append([entry(6)], prev_index=5, prev_term=1)
        assert resp.success is False
        assert resp.conflict_index == 1        # "start from my first slot"

    def test_rejects_when_previous_term_differs(self):
        self.append([entry(1, 1), entry(2, 1), entry(3, 2)], term=2)
        resp = self.append([entry(4, 3)], term=3, prev_index=3, prev_term=3)
        assert resp.success is False
        assert resp.conflict_index == 3        # first index of the conflicting term 2

    def test_overwrites_conflicting_tail(self):
        # Entry 3 came from a leader that died before replicating it
        self.append([entry(1), entry(2), entry(3, 1, "SET lost value")])
        resp = self.append([entry(3, 2, "SET kept value")], term=2, prev_index=2, prev_term=1)
        assert resp.success is True
        node = self.engine._node
        assert node.last_log_index() == 3
        assert node.entry_at(3).command == "SET kept value"

    def test_old_request_does_not_truncate_newer_entries(self):
        self.append([entry(1), entry(2), entry(3)])
        resp = self.append([entry(1)])         # delayed duplicate
        assert resp.success is True
        assert self.engine._node.last_log_index() == 3

    def test_commit_limited_to_entries_this_request_verified(self):
        self.append([entry(1), entry(2)], commit=10)
        assert self.engine._node.commit_index == 2

    def test_commit_index_never_moves_backwards(self):
        self.append([entry(1), entry(2), entry(3)], commit=3)
        self.append([], prev_index=1, prev_term=1, commit=5)
        assert self.engine._node.commit_index == 3

    def test_follower_learns_who_the_leader_is(self):
        self.append()
        assert self.engine.get_leader() == "leader"

    def test_same_term_step_down_keeps_vote(self):
        with self.engine._state_lock:
            self.engine._node.state = RaftState.CANDIDATE
            self.engine._node.current_term = 3
            self.engine._node.voted_for = self.engine.node_id
        self.append(term=3)
        assert self.engine.get_state() == RaftState.FOLLOWER
        assert self.engine._node.voted_for == self.engine.node_id   # no double vote

    def test_leader_stepping_down_fires_callback(self):
        callback = MagicMock()
        engine, _ = make_engine(on_follower=callback)
        with engine._state_lock:
            engine._node.state = RaftState.LEADER
            engine._node.current_term = 1
        engine._handle_append_entries(AppendEntriesRequest(
            term=2, leader_id="new_leader", prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0))
        assert engine.get_state() == RaftState.FOLLOWER
        callback.assert_called_once()


class TestElectionTimer:

    def test_stale_timer_is_ignored(self):
        """A timer that fired just as a heartbeat reset it must not start an election."""
        engine, _ = make_engine()
        engine._running = True
        engine._timer_generation = 5
        engine._start_election(generation=4)
        assert engine.get_term() == 0
        assert engine.get_state() == RaftState.FOLLOWER
        engine._start_election(generation=5)
        assert engine.get_term() == 1
        engine.stop()


class TestRaftPersistence:
    """Week 6: term, vote and log survive a restart."""

    def make(self, storage):
        return RaftEngine("n1", "127.0.0.1", get_free_port(), ["127.0.0.1:1"], storage=storage)

    def test_term_vote_and_log_survive_restart(self):
        storage = MemoryRaftStorage()
        before = self.make(storage)
        before._handle_request_vote(RequestVoteRequest(
            term=4, candidate_id="n2", last_log_index=0, last_log_term=0))
        before._handle_append_entries(AppendEntriesRequest(
            term=4, leader_id="n2", prev_log_index=0, prev_log_term=0,
            entries=[entry(1, 4)], leader_commit=0))

        after = self.make(storage)
        assert after.get_term() == 4
        assert after._node.voted_for == "n2"
        assert after._node.last_log_index() == 1

    def test_cannot_vote_twice_in_a_term_after_restart(self):
        storage = MemoryRaftStorage()
        self.make(storage)._handle_request_vote(RequestVoteRequest(
            term=4, candidate_id="n2", last_log_index=0, last_log_term=0))
        resp = self.make(storage)._handle_request_vote(RequestVoteRequest(
            term=4, candidate_id="n3", last_log_index=0, last_log_term=0))
        assert resp.vote_granted is False

    def test_truncated_entries_stay_truncated(self):
        storage = MemoryRaftStorage()
        engine = self.make(storage)
        engine._handle_append_entries(AppendEntriesRequest(
            term=1, leader_id="a", prev_log_index=0, prev_log_term=0,
            entries=[entry(1), entry(2), entry(3)], leader_commit=0))
        engine._handle_append_entries(AppendEntriesRequest(
            term=2, leader_id="b", prev_log_index=1, prev_log_term=1,
            entries=[entry(2, 2, "SET new 1")], leader_commit=0))
        restored = self.make(storage)._node
        assert restored.last_log_index() == 2
        assert restored.entry_at(2).command == "SET new 1"


class TestLogReplication:
    """Real clusters over TCP."""

    def setup_method(self):
        self.cluster = None

    def teardown_method(self):
        if self.cluster:
            self.cluster.stop_all()

    def test_single_node_commits_immediately(self):
        machine = KVMachine()
        engine = RaftEngine("solo", "127.0.0.1", None, [], apply_fn=machine.apply)
        engine.start()
        try:
            assert engine.propose("SET a 1", "u", timeout=1.0) is True
            assert machine.data == {"a": "1"}
        finally:
            engine.stop()

    def test_entries_reach_every_node_in_the_same_order(self):
        self.cluster = c = RaftCluster()
        c.start()
        leader = c.leader()
        for i in range(20):
            leader.propose(f"SET k{i} v{i}", "u")
        assert wait_until(lambda: all(m.data.get("k19") == "v19" for m in c.machines))
        assert all(m.applied == c.machines[0].applied for m in c.machines)

    def test_propose_returns_the_apply_result(self):
        self.cluster = c = RaftCluster()
        c.start()
        leader = c.leader()
        assert leader.propose("SET a 1", "u") is True
        assert leader.propose("DELETE a", "u") is True
        assert leader.propose("DELETE a", "u") is False

    def test_propose_on_follower_names_the_leader(self):
        self.cluster = c = RaftCluster()
        c.start()
        leader = c.leader()
        follower = next(e for e in c.engines if e is not leader)
        assert wait_until(lambda: follower.get_leader() == leader.node_id)
        with pytest.raises(NotLeaderError) as err:
            follower.propose("SET a 1", "u")
        assert err.value.leader_id == leader.node_id

    def test_restarted_node_catches_up(self):
        self.cluster = c = RaftCluster()
        c.start()
        leader = c.leader()
        lagging = next(i for i, e in enumerate(c.engines) if e is not leader)
        c.engines[lagging].stop()
        for i in range(10):
            leader.propose(f"SET k{i} v{i}", "u")
        c.restart(lagging)
        expected = c.machine_of(leader).data
        assert wait_until(lambda: c.machines[lagging].data == expected)

    def test_committed_entries_survive_leader_crash(self):
        self.cluster = c = RaftCluster()
        c.start()
        old = c.leader()
        for i in range(5):
            old.propose(f"SET k{i} v{i}", "u")
        old.stop()
        survivors = [i for i, e in enumerate(c.engines) if e is not old]
        new = c.leader(among=survivors)
        new.propose("SET after crash", "u")
        for i in survivors:
            assert wait_until(lambda: c.machines[i].data.get("after") == "crash")
            assert all(c.machines[i].data.get(f"k{n}") == f"v{n}" for n in range(5))

    def test_no_majority_means_no_commit(self):
        self.cluster = c = RaftCluster()
        c.start()
        leader = c.leader()
        for e in c.engines:
            if e is not leader:
                e.stop()
        with pytest.raises(ProposalError):
            leader.propose("SET lonely 1", "u", timeout=0.5)
        assert "lonely" not in c.machine_of(leader).data


class TestSnapshots:

    def setup_method(self):
        self.cluster = None

    def teardown_method(self):
        if self.cluster:
            self.cluster.stop_all()

    def test_log_is_compacted_after_threshold(self):
        machine = KVMachine()
        engine = RaftEngine("solo", "127.0.0.1", None, [], apply_fn=machine.apply,
                            snapshot_threshold=10)
        engine.start()
        try:
            for i in range(25):
                engine.propose(f"SET k{i} v{i}", "u")
            node = engine._node
            assert node.snapshot_index >= 20
            assert len(node.log) < 10
            assert machine.data["k24"] == "v24"
        finally:
            engine.stop()

    def test_new_node_is_brought_up_to_date_with_a_snapshot(self):
        self.cluster = c = RaftCluster(snapshot_threshold=5)
        c.start(0, 1)                                   # node3 is "down"
        leader = c.leader(among=[0, 1])
        for i in range(20):
            leader.propose(f"SET k{i} v{i}", "u")
        assert leader._node.snapshot_index > 0

        c.start(2)
        expected = c.machine_of(leader).data
        assert wait_until(lambda: c.machines[2].data == expected)
        # it got most of the data via restore(), not one entry at a time
        assert len(c.machines[2].applied) < 20