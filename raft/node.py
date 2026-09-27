"""
raft/node.py

The Raft consensus node — leader election, log replication, persistence,
snapshots, membership changes and linearizable reads.

This is the hardest file in the entire project. Read every comment.

THE BIG PICTURE
Every write (SIGNUP / SET / DELETE ...) becomes a LogEntry. The leader
puts it in its log, copies it to the followers, and once a MAJORITY of
nodes store it the entry is "committed". Every node then hands committed
entries, strictly in order, to apply_fn. Same entries + same order =
every node ends up with exactly the same cache.

HOW LEADER ELECTION WORKS (Week 4):
1. All nodes start as FOLLOWERs
2. Each follower has an election timeout (random 1.5s - 3s)
3. If a follower doesn't hear a heartbeat before timeout:
   → it increments its term
   → becomes CANDIDATE
   → votes for itself
   → sends RequestVote to all peers
4. If candidate gets votes from majority (2 out of 3 nodes):
   → becomes LEADER
   → immediately starts sending heartbeats every 500ms
5. If a node sees a higher term:
   → immediately becomes FOLLOWER

WHY RANDOM TIMEOUTS?
If all nodes had the same timeout, they'd ALL start elections
simultaneously and split votes forever. Random timeouts mean
one node almost always starts the election first and wins.

HOW LOG REPLICATION WORKS (Week 5):
1. A client write reaches the leader → propose() appends it to the log.
2. One replicator thread per follower sends the entries that follower is
   missing, plus (prev_log_index, prev_log_term) of the entry just before.
3. CONSISTENCY CHECK: the follower accepts only if it has that previous
   entry. If not, it says so and the leader backs up (next_index) until
   the two logs agree, then overwrites the follower's conflicting tail.
4. When a majority stores an entry from the CURRENT term the leader
   advances commit_index; followers learn it from leader_commit.
5. The applier thread feeds committed entries to apply_fn, in order.

PERSISTENCE (Week 6):
current_term, voted_for and the log go through a RaftStorage (MySQL in a
real cluster) BEFORE we answer an RPC that depends on them.

SNAPSHOTS:
The log would otherwise grow forever. After `snapshot_threshold` applied
entries we drop them: their effect already lives in the state machine.
A follower that needs dropped entries is sent the state machine instead,
streamed in chunks through a temporary file (InstallSnapshot).

PRE-VOTE + CHECK-QUORUM (Week 7, Raft thesis §9.6):
A node cut off from the others would keep starting elections, raising its
term every time; when it reconnected, that huge term would force a
perfectly healthy leader to step down. So before a real election a node
asks "WOULD you vote for me?" (a pre-vote — nobody changes any state).
Nodes that still hear from a leader say no. Only with a majority of yeses
does it increment its term. For the same reason a node that hears from a
live leader ignores RequestVote from anyone else (leader stickiness).
Symmetrically, a leader that hasn't reached a majority for a whole
election timeout steps down (check-quorum) instead of lingering.

LINEARIZABLE READS (ReadIndex, thesis §6.4):
A leader can't answer a read from its own copy blindly: it might have
been replaced by a new leader it hasn't heard of yet. So it notes its
commit index, then proves it is still the leader by getting a majority
to answer a heartbeat sent AFTER the read arrived. Once its state
machine has applied up to that index the read is guaranteed current.
A follower gets the same index from the leader (ReadIndex RPC), waits
until it has applied that far, and answers from its own copy.

WRITE FORWARDING:
A follower hands client writes to the leader (Forward RPC) and returns
the leader's answer, so clients can use any node.

MEMBERSHIP CHANGES (thesis §4, one server at a time):
The membership is itself a log entry (CONFIG). A node always uses the
newest CONFIG in its log, committed or not. Changing by ONE server at a
time guarantees the old and new majorities overlap, so two leaders can
never be elected. A new node first catches up as a non-voting learner,
so adding it doesn't stall commits. A leader that removes itself steps
down once that change is committed.

LEADERSHIP TRANSFER (thesis §3.10):
On a graceful shutdown the leader stops taking writes, makes sure the
most up-to-date follower has its whole log, and tells it to start an
election immediately (TimeoutNow). Failover takes milliseconds instead
of an election timeout.

LOCKING RULES
  _state_lock — guards all Raft state (_node). Held only briefly; no
                network I/O while holding it (storage writes are allowed).
  _apply_lock — held while changing the state machine (apply / snapshot
                install / snapshot dump). Always taken BEFORE _state_lock.
  _config_change_lock — serialises add_member / remove_member calls.
                Always taken BEFORE the other two.
  Callbacks (on_become_leader / on_become_follower) run with _state_lock
  held: keep them short, and never wait on another thread that needs
  this engine.
"""
import base64
import os
import socket
import tempfile
import threading
import time
import random
import logging
from typing import Any, Callable, Optional

from raft.types import (RaftState, RaftNode, LogEntry, Member, NOOP,
                        config_command, parse_config)
from raft.rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    ReadIndexRequest, ReadIndexResponse,
    ForwardRequest, ForwardResponse,
    TimeoutNowRequest, TimeoutNowResponse,
    encode,
    decode_request_vote_req, decode_request_vote_resp,
    decode_append_entries_req, decode_append_entries_resp,
    decode_install_snapshot_req, decode_install_snapshot_resp,
    decode_read_index_req, decode_read_index_resp,
    decode_forward_req, decode_forward_resp,
    decode_timeout_now_req, decode_timeout_now_resp,
)
from raft.storage import RaftStorage, MemoryRaftStorage

logger = logging.getLogger(__name__)

# Timing constants (in seconds)
HEARTBEAT_INTERVAL    = 0.5      # Leader sends heartbeat every 500ms
ELECTION_TIMEOUT_MIN  = 1.5      # Follower waits at least 1.5s
ELECTION_TIMEOUT_MAX  = 3.0      # Follower waits at most 3.0s
RPC_TIMEOUT           = 1.0      # Give up on a peer's reply after 1s
SNAPSHOT_RPC_TIMEOUT  = 10.0     # A snapshot chunk may take longer

MAX_ENTRIES_PER_RPC        = 100          # Batch size when a follower is behind
DEFAULT_SNAPSHOT_THRESHOLD = 1000         # Compact after this many applied entries
SNAPSHOT_CHUNK_SIZE        = 256 * 1024   # Bytes per InstallSnapshot message

_PENDING = object()   # marker: a proposal is still waiting for its result
_STOPPED = object()   # marker: the engine stopped while applying


class NotLeaderError(Exception):
    """This node isn't the leader (leader_id tells who is, if known)."""

    def __init__(self, leader_id: Optional[str]):
        self.leader_id = leader_id
        super().__init__(f"not the leader (current leader: {leader_id or 'unknown'})")


class ProposalError(Exception):
    """A request could not be confirmed (no majority in time, overwritten...)."""


class ConfigChangeError(Exception):
    """A membership change was refused (already a member, last member...)."""


class RaftEngine:
    """
    Core Raft implementation.

    Usage:
        engine = RaftEngine(
            node_id="node1",
            host="127.0.0.1",
            port=9001,
            members=[Member("node1", "127.0.0.1:9001", "127.0.0.1:8001"), ...],
            apply_fn=state_machine.apply,
        )
        engine.start()                                  # RPC server + timers
        result = engine.submit("SET k v", "shubham")    # any node
        engine.read_index()                             # before a read

    Instead of `members` you may pass `peers` (other nodes' Raft
    addresses); their node ids are then their addresses.
    port=None means "don't listen for RPCs" — only valid for a single
    node (the standalone server uses this). join=True starts with no
    membership at all: the node waits for a leader to add it.
    """

    def __init__(self, node_id: str, host: str, port: Optional[int],
                 peers: Optional[list[str]] = None,
                 on_become_leader: Optional[Callable] = None,
                 on_become_follower: Optional[Callable] = None,
                 apply_fn: Optional[Callable[[LogEntry], Any]] = None,
                 snapshot_fn: Optional[Callable[[Any], None]] = None,
                 restore_fn: Optional[Callable[[Any], None]] = None,
                 storage: Optional[RaftStorage] = None,
                 heartbeat_interval: float = HEARTBEAT_INTERVAL,
                 election_timeout: tuple[float, float] = (ELECTION_TIMEOUT_MIN,
                                                          ELECTION_TIMEOUT_MAX),
                 snapshot_threshold: int = DEFAULT_SNAPSHOT_THRESHOLD,
                 members: Optional[list[Member]] = None,
                 join: bool = False,
                 client_address: str = "",
                 ssl_server_context=None,
                 ssl_client_context=None):
        self.node_id = node_id
        self.host    = host
        self.port    = port
        self.raft_address = f"{host}:{port}" if port is not None else ""

        # Callbacks — TCP server can react to state changes
        self.on_become_leader   = on_become_leader
        self.on_become_follower = on_become_follower

        # The state machine (our cache) — see server/state_machine.py.
        # snapshot_fn(file) writes a snapshot, restore_fn(file) reads one.
        self._apply_fn    = apply_fn
        self._snapshot_fn = snapshot_fn
        self._restore_fn  = restore_fn

        self._storage = storage or MemoryRaftStorage()
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout   = election_timeout
        self.snapshot_threshold = snapshot_threshold
        self._ssl_server_context = ssl_server_context    # mutual TLS between nodes
        self._ssl_client_context = ssl_client_context

        # The membership we start with if storage has none saved yet
        if join:
            self._bootstrap: list[Member] = []
        elif members is not None:
            self._bootstrap = list(members)
        else:
            self._bootstrap = ([Member(node_id, self.raft_address, client_address)] +
                               [Member(p, p) for p in (peers or [])])

        # Core Raft state — protected by a single lock
        self._state_lock = threading.RLock()
        self._cond = threading.Condition(self._state_lock)   # "something changed"
        self._apply_lock = threading.Lock()
        self._config_change_lock = threading.Lock()
        self._node = RaftNode(node_id=node_id, peers=[])

        self._members: dict[str, Member] = {}     # current voting membership
        self._config_index = 0                    # log index that defined it
        self._learners: dict[str, Member] = {}    # leader: catching up, not voting yet
        self._term_start_index = 0                # leader: index of our no-op
        self._last_contact: dict[str, float] = {}     # leader: peer → last reply time
        self._last_ack_sent: dict[str, float] = {}    # leader: peer → send time of newest answered request
        self._last_leader_contact = 0.0           # follower: last time a leader talked to us
        self._leader_address = ""                 # follower: leader's Raft address
        self._transferring = False                # leader: handing over, refuse writes
        self._snapshot_rx: Optional[dict] = None  # follower: snapshot being received
        self._blocked: set[str] = set()           # test hook: addresses we "can't reach"

        self._restore_persistent_state()

        # Controls
        self._running = False
        self._election_timer: Optional[threading.Timer] = None
        self._timer_generation = 0
        self._server_socket: Optional[socket.socket] = None
        self._replicators: dict[str, threading.Event] = {}   # peer id → wake-up event
        self._waiters: dict[int, Any] = {}   # log index → _PENDING / (term, result)

    # ──────────────────────────────────────────────
    # PUBLIC API
    # ──────────────────────────────────────────────

    def start(self):
        """Start the Raft engine — RPC server, applier and election timer."""
        self._running = True
        if self.port is not None:
            # Bind here rather than inside the thread, so a port clash is
            # reported to the caller instead of dying silently.
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen(50)
            sock.settimeout(1.0)
            self._server_socket = sock
            self._spawn(self._run_rpc_server, name=f"raft-rpc-{self.node_id}")
        self._spawn(self._apply_loop, name=f"raft-apply-{self.node_id}")

        logger.info(f"[{self.node_id}] Raft engine started on port {self.port}")
        with self._state_lock:
            alone = set(self._members) == {self.node_id}
            if not alone:
                self._reset_election_timer()
        if alone:
            # Alone in the cluster: we are trivially the majority.
            self._start_election()

    def stop(self):
        """Stop the Raft engine."""
        with self._state_lock:
            self._running = False
            self._timer_generation += 1
            if self._election_timer:
                self._election_timer.cancel()
            self._cond.notify_all()
            events = list(self._replicators.values())
            rx, self._snapshot_rx = self._snapshot_rx, None
        for event in events:
            event.set()
        if rx:
            _remove_quietly(rx["path"])
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass

    def get_state(self) -> RaftState:
        with self._state_lock:
            return self._node.state

    def get_term(self) -> int:
        with self._state_lock:
            return self._node.current_term

    def get_leader(self) -> Optional[str]:
        """Return the current leader's node_id, or None if unknown."""
        with self._state_lock:
            if self._node.state == RaftState.LEADER:
                return self.node_id
            return self._node.leader_id

    def is_leader(self) -> bool:
        with self._state_lock:
            return self._node.state == RaftState.LEADER

    @property
    def peers(self) -> list[str]:
        """Raft addresses of the other voting members."""
        with self._state_lock:
            return [m.raft_address for mid, m in self._members.items() if mid != self.node_id]

    def members(self) -> list[Member]:
        """The current voting membership, sorted by node id."""
        with self._state_lock:
            return sorted(self._members.values(), key=lambda m: m.node_id)

    def member(self, node_id: Optional[str]) -> Optional[Member]:
        with self._state_lock:
            return self._members.get(node_id) or self._learners.get(node_id)

    def status(self) -> dict:
        """A snapshot of the interesting numbers, for the INFO command."""
        with self._state_lock:
            n = self._node
            return {
                "node": self.node_id,
                "role": n.state.value,
                "term": n.current_term,
                "leader": self.node_id if n.state == RaftState.LEADER else n.leader_id,
                "commit": n.commit_index,
                "applied": n.last_applied,
                "last_log": n.last_log_index(),
                "snapshot": n.snapshot_index,
                "members": ",".join(sorted(self._members)) or "-",
            }

    def propose(self, command: str, username: str, timeout: float = 5.0,
                request_id: str = "") -> Any:
        """
        LEADER ONLY. Replicate one command and wait until it is committed
        AND applied on this node. Returns whatever apply_fn returned.

        Raises:
            NotLeaderError — this node isn't (or stopped being) the leader
            ProposalError  — no majority within `timeout`, or a new leader
                             replaced the entry. With a request_id a retry
                             is always safe: it is applied at most once.
        """
        with self._state_lock:
            n = self._node
            if not self._running or self._transferring:
                raise NotLeaderError(None)
            if n.state != RaftState.LEADER:
                raise NotLeaderError(n.leader_id)
            term = n.current_term
            entry = LogEntry(term=term, index=n.last_log_index() + 1,
                             command=command, username=username, request_id=request_id)
            self._append_local([entry])
            self._waiters[entry.index] = _PENDING
            self._advance_commit_index()      # a lone node commits right away
            events = list(self._replicators.values())

        for event in events:                  # wake the replicators now
            event.set()

        deadline = time.monotonic() + timeout
        with self._cond:
            try:
                while True:
                    outcome = self._waiters.get(entry.index, _PENDING)
                    if outcome is not _PENDING:
                        applied_term, result = outcome
                        if applied_term != term:
                            raise ProposalError("write was replaced by a new leader, retry")
                        return result
                    if n.state != RaftState.LEADER or n.current_term != term:
                        raise NotLeaderError(n.leader_id)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not self._running:
                        raise ProposalError("timed out waiting for a majority of nodes")
                    self._cond.wait(remaining)
            finally:
                self._waiters.pop(entry.index, None)

    def submit(self, command: str, username: str, request_id: str = "",
               timeout: float = 5.0) -> Any:
        """
        ANY NODE. Like propose(), but a follower forwards the write to the
        leader. Waits out a leader election (up to `timeout`) instead of
        failing, so clients rarely notice a failover.
        """
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                with self._state_lock:
                    leader = self._node.leader_id
                raise NotLeaderError(None) if leader is None else \
                    ProposalError("timed out waiting for the leader")
            with self._state_lock:
                is_leader = self._node.state == RaftState.LEADER
                leader_address = self._leader_address if self._node.leader_id else ""
            try:
                if is_leader:
                    return self.propose(command, username, remaining, request_id)
                if leader_address:
                    response = self._call(leader_address, "Forward",
                                          ForwardRequest(command, username, request_id, remaining),
                                          decode_forward_resp, timeout=remaining + RPC_TIMEOUT)
                    if response is not None and response.ok:
                        return response.result
                    if response is not None and response.error != "not_leader":
                        raise ProposalError(response.error)
            except NotLeaderError:
                pass                     # leadership moved — look again
            time.sleep(0.05)             # election in progress / leader unreachable

    def read_index(self, timeout: float = 5.0) -> int:
        """
        ANY NODE. Linearizable read barrier: returns once this node's state
        machine reflects every write that was committed before the call.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._state_lock:
                is_leader = self._node.state == RaftState.LEADER
            try:
                if is_leader:
                    index = self._confirm_read_index(deadline)
                else:
                    index = self._remote_read_index(deadline)
                break
            except NotLeaderError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        self._wait_applied(index, deadline)
        return index

    def add_member(self, member: Member, timeout: float = 30.0) -> None:
        """
        LEADER ONLY. Add one voting member: it first catches up as a
        learner, then a CONFIG entry makes it a voter. Idempotent.
        """
        deadline = time.monotonic() + timeout
        with self._config_change_lock:
            with self._cond:
                self._require_leader()
                existing = self._members.get(member.node_id)
                if existing is not None:
                    if existing == member:
                        return
                    raise ConfigChangeError(f"{member.node_id} is already a member "
                                            f"with different addresses")
                self._wait_config_idle(deadline)
                n = self._node
                term = n.current_term
                self._learners[member.node_id] = member
                self._start_replicator(member.node_id)
                logger.info(f"[{self.node_id}] Adding {member.node_id}: catching it up first")
                try:
                    # Caught up = within one batch of our log end
                    while n.last_log_index() - n.match_index.get(member.node_id, 0) > MAX_ENTRIES_PER_RPC:
                        self._wait_leader(term, deadline,
                                          f"{member.node_id} did not catch up in time")
                    members = list(self._members.values()) + [member]
                except Exception:
                    self._learners.pop(member.node_id, None)
                    raise
            try:
                self.propose(config_command(members), "",
                             max(deadline - time.monotonic(), 1.0))
            finally:
                with self._state_lock:
                    if member.node_id not in self._members:
                        self._learners.pop(member.node_id, None)
        logger.info(f"[{self.node_id}] {member.node_id} is now a voting member")

    def remove_member(self, node_id: str, timeout: float = 30.0) -> None:
        """LEADER ONLY. Remove one voting member (may be this node)."""
        deadline = time.monotonic() + timeout
        with self._config_change_lock:
            with self._cond:
                self._require_leader()
                if node_id not in self._members:
                    raise ConfigChangeError(f"{node_id} is not a member")
                if len(self._members) == 1:
                    raise ConfigChangeError("cannot remove the last member")
                self._wait_config_idle(deadline)
                members = [m for m in self._members.values() if m.node_id != node_id]
            self.propose(config_command(members), "",
                         max(deadline - time.monotonic(), 1.0))
        logger.info(f"[{self.node_id}] {node_id} removed from the cluster")

    def transfer_leadership(self, timeout: float = 3.0) -> bool:
        """
        LEADER ONLY. Hand leadership to the most up-to-date follower.
        Returns True once another node has taken over.
        """
        deadline = time.monotonic() + timeout
        with self._cond:
            n = self._node
            voters = [m for m in self._members if m != self.node_id]
            if n.state != RaftState.LEADER or not voters:
                return False
            self._transferring = True
            term = n.current_term
            target = max(voters, key=lambda p: n.match_index.get(p, 0))
            for event in self._replicators.values():
                event.set()
        try:
            with self._cond:
                while n.match_index.get(target, 0) < n.last_log_index():
                    remaining = deadline - time.monotonic()
                    if n.state != RaftState.LEADER or n.current_term != term or remaining <= 0:
                        return False
                    self._cond.wait(remaining)
                address = self._members[target].raft_address
            logger.info(f"[{self.node_id}] Handing leadership to {target}")
            if self._call(address, "TimeoutNow", TimeoutNowRequest(term, self.node_id),
                          decode_timeout_now_resp) is None:
                return False
            with self._cond:
                while n.state == RaftState.LEADER:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._cond.wait(remaining)
            return True
        finally:
            with self._state_lock:
                self._transferring = False

    # ──────────────────────────────────────────────
    # ELECTION TIMER
    # ──────────────────────────────────────────────

    def _reset_election_timer(self):
        """
        Reset the election timeout with a NEW random duration.
        Called (with _state_lock held):
          - On startup
          - When we receive a valid heartbeat
          - When we grant a vote
          - When we become a follower
          - When we start a (pre-)election (in case it goes nowhere)

        Every reset bumps a generation number. A timer that already fired
        but hasn't grabbed the lock yet sees a newer generation and gives
        up — cancel() alone can't stop a timer that is already running.
        The same number lets a pre-vote round notice that something
        (a heartbeat, a vote we granted) happened while it was running.
        """
        self._timer_generation += 1
        if self._election_timer:
            self._election_timer.cancel()

        if not self._running:
            return

        timeout = random.uniform(*self.election_timeout)
        self._election_timer = threading.Timer(
            timeout, self._on_election_timeout, args=(self._timer_generation,))
        self._election_timer.daemon = True
        self._election_timer.start()

    def _on_election_timeout(self, generation: int):
        try:
            self._start_election(generation)
        finally:
            self._storage.close_thread_resources()

    def _start_election(self, generation: Optional[int] = None):
        """
        Election timeout fired — no heartbeat received in time.
        Runs the PRE-VOTE round first; only if a majority would vote for
        us do we become a CANDIDATE and hold the real election.
        """
        with self._state_lock:
            if not self._running:
                return
            if generation is not None and generation != self._timer_generation:
                return      # stale timer — a heartbeat arrived just in time
            # Only followers and candidates can start elections
            if self._node.state == RaftState.LEADER:
                return
            if self.node_id not in self._members:
                self._reset_election_timer()   # not a voter (yet): nothing to campaign for
                return

            voters = [m for m in self._members.values() if m.node_id != self.node_id]
            if not voters:
                self._become_candidate()       # single-node cluster
                return

            n = self._node
            self._reset_election_timer()       # try again later if this goes nowhere
            round_id = self._timer_generation
            request = RequestVoteRequest(
                term=n.current_term + 1,
                candidate_id=self.node_id,
                last_log_index=n.last_log_index(),
                last_log_term=n.last_log_term(),
                pre_vote=True,
            )
            grants = {self.node_id}

        logger.debug(f"[{self.node_id}] Pre-vote for term {request.term}")
        for peer in voters:
            self._spawn(self._request_pre_vote_from, peer, request, grants, round_id)

    def _request_pre_vote_from(self, peer: Member, request: RequestVoteRequest,
                               grants: set, round_id: int):
        response = self._send_request_vote(peer.raft_address, request)
        if response is None:
            return
        with self._state_lock:
            n = self._node
            if response.term > n.current_term:
                self._become_follower(response.term)   # we were behind; catch up
                return
            if round_id != self._timer_generation or n.state == RaftState.LEADER:
                return      # a leader spoke up, or we voted, since we asked
            if not response.vote_granted:
                return
            grants.add(peer.node_id)
            if len(grants & self._members.keys()) >= self._majority():
                self._become_candidate()

    def _become_candidate(self, transfer: bool = False):
        """
        The real election. Transition: FOLLOWER → CANDIDATE and request
        votes. Votes are counted as they arrive; we become leader the
        moment we reach a majority instead of waiting for slow peers.
        Called with _state_lock held.
        """
        # Increment term and vote for self
        n = self._node
        n.current_term += 1
        n.state        = RaftState.CANDIDATE
        n.voted_for    = self.node_id
        n.leader_id    = None
        self._persist_term_and_vote()
        votes = {self.node_id}

        logger.info(f"[{self.node_id}] Starting election for term {n.current_term}")

        if len(votes & self._members.keys()) >= self._majority():
            self._become_leader()
            return

        self._reset_election_timer()           # retry if this one stalls
        request = RequestVoteRequest(
            term=n.current_term,
            candidate_id=self.node_id,
            last_log_index=n.last_log_index(),
            last_log_term=n.last_log_term(),
            transfer=transfer,
        )
        # Send RequestVote to all peers in parallel
        for peer in self._members.values():
            if peer.node_id != self.node_id:
                self._spawn(self._request_vote_from, peer, request, votes)

    def _request_vote_from(self, peer: Member, request: RequestVoteRequest, votes: set):
        response = self._send_request_vote(peer.raft_address, request)
        if response is None:
            return

        with self._state_lock:
            # If we see a higher term, immediately step down
            if response.term > self._node.current_term:
                self._become_follower(response.term)
                return
            if (self._node.state != RaftState.CANDIDATE or
                    self._node.current_term != request.term or
                    not response.vote_granted):
                return      # election already decided, or vote refused
            votes.add(peer.node_id)
            counted = len(votes & self._members.keys())
            logger.info(f"[{self.node_id}] Got vote from {peer.node_id} "
                        f"({counted}/{self._majority()} needed)")
            if counted >= self._majority():
                self._become_leader()

    def _majority(self) -> int:
        """Votes / replicas needed: 2 of 3, 3 of 5, 1 of 1..."""
        return len(self._members) // 2 + 1

    # ──────────────────────────────────────────────
    # STATE TRANSITIONS  (all called with _state_lock held)
    # ──────────────────────────────────────────────

    def _become_leader(self):
        """We won the election. Transition to LEADER."""
        n = self._node
        n.state = RaftState.LEADER
        n.leader_id = self.node_id
        self._leader_address = self.raft_address
        self._transferring = False
        self._learners = {}

        # Cancel election timer — leaders don't need it
        self._timer_generation += 1
        if self._election_timer:
            self._election_timer.cancel()

        # Initialize leader tracking for each peer: optimistically assume
        # they have everything we have; the consistency check corrects us.
        next_idx = n.last_log_index() + 1
        now = time.monotonic()
        n.next_index, n.match_index = {}, {}
        self._last_contact, self._last_ack_sent = {}, {}
        for peer_id in self._targets():
            n.next_index[peer_id]  = next_idx
            n.match_index[peer_id] = 0
            self._last_contact[peer_id] = now      # grace period for check-quorum

        # Commit a no-op from our own term (see NOOP in raft/types.py)
        self._term_start_index = next_idx
        self._append_local([LogEntry(term=n.current_term, index=next_idx,
                                     command=NOOP, username="")])

        logger.info(f"[{self.node_id}] *** BECAME LEADER for term "
                    f"{n.current_term} ***")

        if self.on_become_leader:
            self.on_become_leader()

        self._advance_commit_index()

        # One replicator per follower — doubles as the heartbeat
        self._replicators = {}
        for peer_id in self._targets():
            self._start_replicator(peer_id)
        self._spawn(self._check_quorum_loop, n.current_term,
                    name=f"check-quorum-{self.node_id}")

    def _become_follower(self, term: int):
        """
        Step down to follower (saw a higher term, a valid leader, or lost
        the majority). voted_for is only cleared when the term really
        changes — clearing it inside the same term would allow voting twice.
        """
        n = self._node
        was_leader = n.state == RaftState.LEADER
        if term > n.current_term:
            n.current_term = term
            n.voted_for    = None
            n.leader_id    = None
            self._persist_term_and_vote()
        if was_leader:
            n.leader_id = None
            self._learners = {}
            self._transferring = False
        if n.state != RaftState.FOLLOWER:
            logger.info(f"[{self.node_id}] Became follower for term {n.current_term}")
        n.state = RaftState.FOLLOWER

        if was_leader and self.on_become_follower:
            self.on_become_follower()

        self._reset_election_timer()
        self._cond.notify_all()     # wake proposals waiting on our leadership

    def _accept_leader(self, term: int, leader_id: str, leader_address: str):
        """A valid AppendEntries / InstallSnapshot arrived (term >= ours)."""
        n = self._node
        if term > n.current_term or n.state != RaftState.FOLLOWER:
            self._become_follower(term)
        else:
            self._reset_election_timer()   # reset — we heard from leader
        if n.leader_id != leader_id:
            logger.info(f"[{self.node_id}] Following leader {leader_id} (term {term})")
            n.leader_id = leader_id
        if leader_address:
            self._leader_address = leader_address
        elif leader_id in self._members:
            self._leader_address = self._members[leader_id].raft_address
        n.last_heartbeat = time.time()
        self._last_leader_contact = time.monotonic()

    def _in_leader_lease(self) -> bool:
        """
        True while a live leader is known: a follower heard from it within
        the minimum election timeout, or we ARE that leader and still in
        touch with a majority. Votes for anyone else are refused meanwhile.
        """
        if self._node.state == RaftState.LEADER:
            return self._has_quorum_contact(self.election_timeout[0])
        return (self._node.leader_id is not None and
                time.monotonic() - self._last_leader_contact < self.election_timeout[0])

    def _persist_term_and_vote(self):
        self._storage.save_term_and_vote(self._node.current_term, self._node.voted_for)

    # ──────────────────────────────────────────────
    # MEMBERSHIP  (called with _state_lock held)
    # ──────────────────────────────────────────────

    def _targets(self) -> dict[str, Member]:
        """Everyone the leader replicates to: voters and learners, minus us."""
        targets = dict(self._members)
        targets.update(self._learners)
        targets.pop(self.node_id, None)
        return targets

    def _refresh_config(self):
        """Adopt the newest membership in our log (called after it changes)."""
        index, members = self._node.latest_config()
        new = {m.node_id: m for m in (members or [])}
        self._config_index = index
        if new == self._members:
            return
        self._members = new
        logger.info(f"[{self.node_id}] Cluster members: "
                    f"{', '.join(sorted(new)) or '(none)'}")
        for node_id in list(self._learners):
            if node_id in new:
                del self._learners[node_id]     # promoted to voter
        if self._node.state == RaftState.LEADER:
            for peer_id in self._targets():
                self._start_replicator(peer_id)

    def _require_leader(self):
        if not self._running or self._node.state != RaftState.LEADER:
            raise NotLeaderError(None if self._node.state == RaftState.LEADER
                                 else self._node.leader_id)

    def _wait_config_idle(self, deadline: float):
        """
        One change at a time: wait until our own no-op and the latest
        CONFIG entry are both committed. (cond held)
        """
        n = self._node
        term = n.current_term
        while n.commit_index < max(self._term_start_index, self._config_index):
            self._wait_leader(term, deadline, "another membership change is still in progress")

    def _wait_leader(self, term: int, deadline: float, timeout_message: str):
        """Wait on the condition while we stay leader of `term`. (cond held)"""
        n = self._node
        if not self._running or n.state != RaftState.LEADER or n.current_term != term:
            raise NotLeaderError(None if n.leader_id == self.node_id else n.leader_id)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ProposalError(timeout_message)
        self._cond.wait(min(remaining, 0.1))

    # ──────────────────────────────────────────────
    # REPLICATION (Leader only)
    # ──────────────────────────────────────────────

    def _start_replicator(self, peer_id: str):
        """Start the replicator for one peer if it isn't running. (lock held)"""
        if peer_id in self._replicators:
            return
        n = self._node
        if peer_id not in n.next_index:
            n.next_index[peer_id]  = n.last_log_index() + 1
            n.match_index[peer_id] = 0
            self._last_contact[peer_id] = time.monotonic()
        event = threading.Event()
        self._replicators[peer_id] = event
        self._spawn(self._replicate_loop, peer_id, n.current_term, event,
                    name=f"replicate-{self.node_id}->{peer_id}")

    def _replicate_loop(self, peer_id: str, term: int, wakeup: threading.Event):
        """
        Keeps ONE follower in sync for as long as we lead `term` and it is
        a member (or learner).

        Each round sends whatever the follower is missing (nothing = a
        heartbeat). Then it waits for new entries (propose() sets
        `wakeup`) or HEARTBEAT_INTERVAL, whichever comes first.
        One thread per peer means requests to a peer never overlap,
        so next_index/match_index updates can't arrive out of order.
        """
        while True:
            wakeup.clear()
            with self._state_lock:
                n = self._node
                peer = self._targets().get(peer_id)
                if (not self._running or n.state != RaftState.LEADER or
                        n.current_term != term or peer is None):
                    if self._replicators.get(peer_id) is wakeup:
                        del self._replicators[peer_id]
                    return
                next_idx = n.next_index[peer_id]
                request = None
                if next_idx > n.snapshot_index:
                    prev_index = next_idx - 1
                    request = AppendEntriesRequest(
                        term=term,
                        leader_id=self.node_id,
                        prev_log_index=prev_index,
                        prev_log_term=n.term_at(prev_index),
                        entries=n.entries_from(next_idx, MAX_ENTRIES_PER_RPC),
                        leader_commit=n.commit_index,
                        leader_address=self.raft_address,
                    )

            if request is None:
                reachable = self._send_snapshot_to(peer, term)
            else:
                reachable = self._send_entries_to(peer, term, request)

            with self._state_lock:
                behind = (reachable and self._node.state == RaftState.LEADER and
                          self._node.next_index.get(peer_id, 0) <= self._node.last_log_index())
            if not behind:
                wakeup.wait(self.heartbeat_interval)

    def _send_entries_to(self, peer: Member, term: int,
                         request: AppendEntriesRequest) -> bool:
        """Send one AppendEntries and process the reply. False if unreachable."""
        sent_at = time.monotonic()
        response = self._send_append_entries(peer.raft_address, request)
        if response is None:
            return False

        with self._state_lock:
            n = self._node
            if response.term > n.current_term:
                self._become_follower(response.term)
                return True
            if n.state != RaftState.LEADER or n.current_term != term:
                return True
            self._note_contact(peer.node_id, sent_at)

            if response.success:
                match = max(request.prev_log_index + len(request.entries),
                            response.match_index)
                n.match_index[peer.node_id] = max(n.match_index.get(peer.node_id, 0), match)
                n.next_index[peer.node_id]  = n.match_index[peer.node_id] + 1
                self._advance_commit_index()
            else:
                # Logs disagree at prev_log_index — back up and try again.
                hint = response.conflict_index or request.prev_log_index
                n.next_index[peer.node_id] = max(1, min(hint, request.prev_log_index))
            self._cond.notify_all()
        return True

    def _note_contact(self, peer_id: str, sent_at: float):
        """A peer answered a request of our term that we sent at `sent_at`. (lock held)"""
        self._last_contact[peer_id] = time.monotonic()
        self._last_ack_sent[peer_id] = max(self._last_ack_sent.get(peer_id, 0.0), sent_at)
        self._cond.notify_all()      # read_index() may be waiting for this

    def _send_snapshot_to(self, peer: Member, term: int) -> bool:
        """The follower needs entries we compacted: stream it the state machine."""
        path = None
        try:
            with self._apply_lock:              # freeze the state machine
                with self._state_lock:
                    n = self._node
                    if n.state != RaftState.LEADER or n.current_term != term:
                        return True
                    index = n.last_applied
                    last_term = n.term_at(index)
                    config = [m.to_dict() for m in (n.config_at(index) or [])]
                path = self._write_snapshot_file()

            size = os.path.getsize(path)
            logger.info(f"[{self.node_id}] Sending snapshot (up to entry {index}, "
                        f"{size} bytes) to {peer.node_id}")
            sent_at = time.monotonic()
            with open(path, "rb") as f:
                offset = 0
                while True:
                    chunk = f.read(SNAPSHOT_CHUNK_SIZE)
                    done = offset + len(chunk) >= size
                    response = self._send_install_snapshot(peer.raft_address, InstallSnapshotRequest(
                        term=term, leader_id=self.node_id,
                        last_included_index=index, last_included_term=last_term,
                        offset=offset, data=base64.b64encode(chunk).decode("ascii"),
                        done=done, config=config, leader_address=self.raft_address,
                    ))
                    if response is None:
                        return False
                    with self._state_lock:
                        if response.term > self._node.current_term:
                            self._become_follower(response.term)
                            return True
                        if self._node.state != RaftState.LEADER or self._node.current_term != term:
                            return True
                        self._note_contact(peer.node_id, sent_at)
                    if not response.success:
                        return False            # out of order — start over next round
                    offset += len(chunk)
                    if done:
                        break

            with self._state_lock:
                n = self._node
                if n.state == RaftState.LEADER and n.current_term == term:
                    n.match_index[peer.node_id] = max(n.match_index.get(peer.node_id, 0), index)
                    n.next_index[peer.node_id]  = n.match_index[peer.node_id] + 1
                    self._advance_commit_index()
                    self._cond.notify_all()
            return True
        finally:
            if path:
                _remove_quietly(path)

    def _write_snapshot_file(self) -> str:
        """Dump the state machine to a temporary file. (_apply_lock held)"""
        fd, path = tempfile.mkstemp(prefix=f"raft-snapshot-{self.node_id}-", suffix=".jsonl")
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            if self._snapshot_fn:
                self._snapshot_fn(out)
        return path

    def _advance_commit_index(self):
        """
        Leader: commit the highest index stored on a majority of the
        CURRENT membership. Only entries from OUR term are counted
        (Raft §5.4.2) — older entries become committed along with them.
        Called with _state_lock held.
        """
        n = self._node
        if n.state != RaftState.LEADER:
            return
        for index in range(n.last_log_index(), n.commit_index, -1):
            if n.term_at(index) != n.current_term:
                break
            replicas = sum(1 for m in self._members
                           if m == self.node_id or n.match_index.get(m, 0) >= index)
            if replicas >= self._majority():
                n.commit_index = index
                self._cond.notify_all()     # wake the applier
                # Tell followers now rather than at the next heartbeat,
                # so they apply (and can serve) the entry sooner.
                for event in self._replicators.values():
                    event.set()
                break

    def _check_quorum_loop(self, term: int):
        """Leader: step down if a majority hasn't answered for a whole election timeout."""
        while True:
            time.sleep(self.heartbeat_interval)
            with self._state_lock:
                n = self._node
                if not self._running or n.state != RaftState.LEADER or n.current_term != term:
                    return
                if not self._has_quorum_contact(self.election_timeout[1]):
                    logger.warning(f"[{self.node_id}] Lost contact with a majority - stepping down")
                    self._become_follower(n.current_term)
                    return

    def _has_quorum_contact(self, window: float) -> bool:
        """Did a majority of voters (counting us) answer within `window` seconds?"""
        now = time.monotonic()
        count = sum(1 for m in self._members
                    if m == self.node_id or now - self._last_contact.get(m, -1e9) < window)
        return count >= self._majority()

    # ──────────────────────────────────────────────
    # LINEARIZABLE READS
    # ──────────────────────────────────────────────

    def _confirm_read_index(self, deadline: float) -> int:
        """Leader side of ReadIndex (see the module docstring)."""
        start = time.monotonic()
        with self._cond:
            n = self._node
            self._require_leader()
            term = n.current_term
            # (1) Until our own no-op commits we don't know what's committed.
            while n.commit_index < self._term_start_index:
                self._wait_leader(term, deadline, "timed out confirming leadership")
            read_index = n.commit_index
            # (2) Prove we're still leader: a majority answered a request sent after `start`.
            for event in self._replicators.values():
                event.set()
            while True:
                acks = sum(1 for m in self._members
                           if m == self.node_id or self._last_ack_sent.get(m, -1e9) >= start)
                if acks >= self._majority():
                    return read_index
                self._wait_leader(term, deadline, "timed out confirming leadership")

    def _remote_read_index(self, deadline: float) -> int:
        """Follower side of ReadIndex: ask the leader."""
        with self._state_lock:
            term = self._node.current_term
            leader_address = self._leader_address if self._node.leader_id else ""
        if not leader_address:
            raise NotLeaderError(None)
        remaining = max(deadline - time.monotonic(), 0.1)
        response = self._call(leader_address, "ReadIndex",
                              ReadIndexRequest(term, self.node_id),
                              decode_read_index_resp, timeout=remaining)
        if response is None or not response.success:
            raise NotLeaderError(None)
        return response.index

    def _wait_applied(self, index: int, deadline: float):
        with self._cond:
            while self._node.last_applied < index:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._running:
                    raise ProposalError("timed out catching up with the leader")
                self._cond.wait(remaining)

    # ──────────────────────────────────────────────
    # APPLYING COMMITTED ENTRIES
    # ──────────────────────────────────────────────

    def _apply_loop(self):
        """
        The only thread that changes the state machine through the log.
        Applies entries last_applied+1 .. commit_index, one at a time,
        in order, then wakes any propose() waiting for that index.
        """
        while True:
            with self._cond:
                while self._running and self._node.last_applied >= self._node.commit_index:
                    self._cond.wait(0.5)
                if not self._running:
                    return

            with self._apply_lock:
                with self._state_lock:
                    index = self._node.last_applied + 1
                    if index > self._node.commit_index:
                        continue    # a snapshot install got there first
                    entry = self._node.entry_at(index)
                if entry is None:
                    logger.error(f"[{self.node_id}] Committed entry {index} missing from log")
                    time.sleep(0.1)
                    continue

                result = self._apply_with_retry(entry)
                if result is _STOPPED:
                    return

                with self._state_lock:
                    n = self._node
                    n.last_applied = max(n.last_applied, index)
                    if index in self._waiters:
                        self._waiters[index] = (entry.term, result)
                    self._cond.notify_all()
                    if (parse_config(entry.command) is not None and
                            n.state == RaftState.LEADER and
                            self.node_id not in self._members and
                            n.last_applied >= self._config_index):
                        logger.info(f"[{self.node_id}] No longer a member - stepping down")
                        self._become_follower(n.current_term)
                    self._maybe_snapshot()

    def _apply_with_retry(self, entry: LogEntry) -> Any:
        """
        Apply one entry. An exception here means infrastructure trouble
        (e.g. MySQL is down), not a bad command — skipping the entry would
        make this node's data diverge, so we retry until it works.
        """
        if entry.command == NOOP or self._apply_fn is None or parse_config(entry.command) is not None:
            return None
        delay = 0.1
        while self._running:
            try:
                return self._apply_fn(entry)
            except Exception as e:
                verb = entry.command.split(" ", 1)[0]
                logger.error(f"[{self.node_id}] Applying entry {entry.index} ({verb}) "
                             f"failed: {e} - retrying in {delay:.1f}s")
                time.sleep(delay)
                delay = min(delay * 2, 5.0)
        return _STOPPED

    def _maybe_snapshot(self):
        """
        Compact the log once enough entries are applied.
        Safe because apply_fn already stored their effect (in MySQL, or in
        RAM for a RAM-only node). Called with _state_lock held.
        """
        n = self._node
        if not self.snapshot_threshold:
            return
        if n.last_applied - n.snapshot_index < self.snapshot_threshold:
            return
        index = n.last_applied
        term = n.term_at(index)
        config = n.config_at(index)
        n.compact_through(index, term, config)
        self._storage.save_snapshot(index, term, n.last_applied, config)
        logger.info(f"[{self.node_id}] Compacted log through entry {index}")

    # ──────────────────────────────────────────────
    # LOG HELPERS  (called with _state_lock held)
    # ──────────────────────────────────────────────

    def _append_local(self, entries: list[LogEntry]):
        """Append to our log (memory + storage); adopt any new membership."""
        self._node.log.extend(entries)
        self._storage.append(entries)
        if any(parse_config(e.command) is not None for e in entries):
            self._refresh_config()

    def _truncate_local(self, index: int):
        """Drop our log from `index` on; a dropped CONFIG entry is undone."""
        self._node.truncate_from(index)
        self._storage.truncate_from(index)
        self._refresh_config()

    # ──────────────────────────────────────────────
    # RPC SERVER (receives incoming RPCs from peers)
    # ──────────────────────────────────────────────

    def _run_rpc_server(self):
        """
        Listen for incoming Raft RPCs from other nodes.
        Each connection is handled in its own thread.
        """
        server_sock = self._server_socket
        while self._running:
            try:
                conn, addr = server_sock.accept()
                self._spawn(self._handle_rpc, conn)
            except socket.timeout:
                continue
            except OSError:
                break

        try:
            server_sock.close()
        except OSError:
            pass

    def _handle_rpc(self, conn: socket.socket):
        """
        Handle one incoming RPC connection.
        Reads the message type, dispatches to the right handler,
        and sends back the response.

        Protocol:
            Line 1: message type ("RequestVote", "AppendEntries", ...)
            Line 2: JSON payload
            Response: JSON payload + newline
        """
        handlers = {
            "RequestVote":     (decode_request_vote_req,     self._handle_request_vote),
            "AppendEntries":   (decode_append_entries_req,   self._handle_append_entries),
            "InstallSnapshot": (decode_install_snapshot_req, self._handle_install_snapshot),
            "ReadIndex":       (decode_read_index_req,       self._handle_read_index),
            "Forward":         (decode_forward_req,          self._handle_forward),
            "TimeoutNow":      (decode_timeout_now_req,      self._handle_timeout_now),
        }
        try:
            conn.settimeout(SNAPSHOT_RPC_TIMEOUT)
            if self._ssl_server_context:
                conn = self._ssl_server_context.wrap_socket(conn, server_side=True)
            with conn.makefile("rb") as reader:
                msg_type = reader.readline().decode().strip()
                payload = reader.readline().decode().strip()
            if not self._running or not msg_type or not payload:
                return      # a stopped node behaves like a crashed one
            handler = handlers.get(msg_type)
            if handler is None:
                return
            decode_fn, handle_fn = handler
            conn.sendall(encode(handle_fn(decode_fn(payload))))
        except Exception as e:
            logger.debug(f"[{self.node_id}] RPC handle error: {e}")
        finally:
            conn.close()

    # ──────────────────────────────────────────────
    # RPC HANDLERS (process incoming RPCs)
    # ──────────────────────────────────────────────

    def _handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        """
        Decide whether to grant a vote to a candidate.

        Grant vote if ALL of:
          1. req.term >= our current_term
          2. we haven't voted for anyone else this term
          3. candidate's log is at least as up-to-date as ours
        ...and nobody is disrupting a live leader (see PRE-VOTE above).
        """
        with self._state_lock:
            if req.pre_vote:
                # "Would you vote for me?" — answer without changing anything
                grant = (req.term > self._node.current_term and
                         not self._in_leader_lease() and self._log_ok(req))
                return RequestVoteResponse(term=self._node.current_term, vote_granted=grant)

            if (req.term > self._node.current_term and not req.transfer and
                    self._in_leader_lease()):
                # We hear from a live leader: ignore this disruptive
                # candidate WITHOUT adopting its term.
                return RequestVoteResponse(term=self._node.current_term, vote_granted=False)

            # If candidate has a higher term, update ours first
            if req.term > self._node.current_term:
                self._become_follower(req.term)

            # Deny if candidate's term is behind ours
            if req.term < self._node.current_term:
                return RequestVoteResponse(
                    term=self._node.current_term,
                    vote_granted=False
                )

            # Check if we already voted for someone else
            already_voted = (
                self._node.voted_for is not None and
                self._node.voted_for != req.candidate_id
            )
            if already_voted:
                return RequestVoteResponse(
                    term=self._node.current_term,
                    vote_granted=False
                )

            if not self._log_ok(req):
                return RequestVoteResponse(
                    term=self._node.current_term,
                    vote_granted=False
                )

            # Grant the vote — persist it BEFORE replying
            self._node.voted_for = req.candidate_id
            self._persist_term_and_vote()
            self._reset_election_timer()   # reset — we heard from a valid node
            logger.info(f"[{self.node_id}] Voted for {req.candidate_id} "
                        f"in term {req.term}")
            return RequestVoteResponse(
                term=self._node.current_term,
                vote_granted=True
            )

    def _log_ok(self, req: RequestVoteRequest) -> bool:
        """Candidate's log is at least as up-to-date as ours."""
        our_last_term  = self._node.last_log_term()
        our_last_index = self._node.last_log_index()
        return (req.last_log_term > our_last_term or
                (req.last_log_term == our_last_term and
                 req.last_log_index >= our_last_index))

    def _handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        """
        Handle AppendEntries from leader (heartbeat or log replication).

          1. Reject stale leaders (req.term < our term).
          2. Consistency check: we must hold prev_log_index with
             prev_log_term, otherwise reply with a conflict_index hint.
          3. Append the new entries, cutting off any conflicting tail.
          4. Advance commit_index from leader_commit.
        """
        with self._state_lock:
            n = self._node
            # Reject stale leaders
            if req.term < n.current_term:
                return AppendEntriesResponse(term=n.current_term, success=False)

            self._accept_leader(req.term, req.leader_id, req.leader_address)

            entries = [e if isinstance(e, LogEntry) else LogEntry(**e) for e in req.entries]
            prev_index, prev_term = req.prev_log_index, req.prev_log_term

            # Entries already inside our snapshot are committed — skip them.
            if prev_index < n.snapshot_index:
                entries = entries[n.snapshot_index - prev_index:]
                prev_index, prev_term = n.snapshot_index, n.snapshot_term

            # CONSISTENCY CHECK
            if prev_index > n.last_log_index():
                return AppendEntriesResponse(term=n.current_term, success=False,
                                             conflict_index=n.last_log_index() + 1)
            if n.term_at(prev_index) != prev_term:
                # Hint: first index of the conflicting term, so the leader
                # can skip that whole term in one round trip.
                conflict_term = n.term_at(prev_index)
                first = prev_index
                while first - 1 > n.snapshot_index and n.term_at(first - 1) == conflict_term:
                    first -= 1
                return AppendEntriesResponse(term=n.current_term, success=False,
                                             conflict_index=first)

            # APPEND — skip entries we already have; a conflicting entry
            # means our tail came from a dead leader and is deleted.
            new_entries = []
            for i, entry in enumerate(entries):
                existing_term = n.term_at(entry.index)
                if existing_term is None:
                    new_entries = entries[i:]
                    break
                if existing_term != entry.term:
                    logger.info(f"[{self.node_id}] Dropping conflicting log entries "
                                f"from {entry.index}")
                    self._truncate_local(entry.index)
                    new_entries = entries[i:]
                    break
            if new_entries:
                self._append_local(new_entries)

            # Only entries this request proved to match may be committed.
            last_new = prev_index + len(entries)
            if req.leader_commit > n.commit_index:
                n.commit_index = max(n.commit_index, min(req.leader_commit, last_new))
                self._cond.notify_all()

            logger.debug(f"[{self.node_id}] AppendEntries from {req.leader_id} "
                         f"term={req.term} entries={len(entries)}")

            return AppendEntriesResponse(term=n.current_term, success=True,
                                         match_index=last_new)

    def _handle_install_snapshot(self, req: InstallSnapshotRequest) -> InstallSnapshotResponse:
        """Receive one snapshot chunk; on the last one, replace our state machine."""
        with self._state_lock:
            n = self._node
            if req.term < n.current_term:
                return InstallSnapshotResponse(term=n.current_term, success=False)
            self._accept_leader(req.term, req.leader_id, req.leader_address)
            if req.last_included_index <= n.last_applied:
                return InstallSnapshotResponse(term=n.current_term)     # already have it

            rx = self._snapshot_rx
            if req.offset == 0:
                if rx:
                    _remove_quietly(rx["path"])
                fd, path = tempfile.mkstemp(prefix=f"raft-install-{self.node_id}-",
                                            suffix=".jsonl")
                os.close(fd)
                rx = self._snapshot_rx = {"index": req.last_included_index, "path": path, "size": 0}
            elif rx is None or rx["index"] != req.last_included_index or rx["size"] != req.offset:
                return InstallSnapshotResponse(term=n.current_term, success=False)

            chunk = base64.b64decode(req.data)
            with open(rx["path"], "ab") as f:
                f.write(chunk)
            rx["size"] += len(chunk)
            if not req.done:
                return InstallSnapshotResponse(term=n.current_term)
            self._snapshot_rx = None
            path = rx["path"]

        try:
            with self._apply_lock:
                with self._state_lock:
                    if req.last_included_index <= self._node.last_applied:
                        return InstallSnapshotResponse(term=self._node.current_term)
                logger.info(f"[{self.node_id}] Installing snapshot up to entry "
                            f"{req.last_included_index} from {req.leader_id}")
                if self._restore_fn:
                    with open(path, encoding="utf-8") as f:
                        self._restore_fn(f)

                with self._state_lock:
                    n = self._node
                    config = [Member.from_dict(d) for d in (req.config or [])]
                    if n.term_at(req.last_included_index) != req.last_included_term:
                        self._storage.truncate_from(n.snapshot_index + 1)
                    n.compact_through(req.last_included_index, req.last_included_term, config)
                    n.commit_index = max(n.commit_index, req.last_included_index)
                    n.last_applied = max(n.last_applied, req.last_included_index)
                    self._storage.save_snapshot(n.snapshot_index, n.snapshot_term,
                                                n.last_applied, config)
                    self._refresh_config()
                    self._cond.notify_all()
                    return InstallSnapshotResponse(term=n.current_term)
        finally:
            _remove_quietly(path)

    def _handle_read_index(self, req: ReadIndexRequest) -> ReadIndexResponse:
        try:
            index = self._confirm_read_index(time.monotonic() + 2 * RPC_TIMEOUT)
            return ReadIndexResponse(term=self.get_term(), success=True, index=index)
        except (NotLeaderError, ProposalError):
            return ReadIndexResponse(term=self.get_term(), success=False)

    def _handle_forward(self, req: ForwardRequest) -> ForwardResponse:
        try:
            result = self.propose(req.command, req.username,
                                  timeout=min(max(req.timeout, 0.1), 30.0),
                                  request_id=req.request_id)
            return ForwardResponse(ok=True, result=result)
        except NotLeaderError:
            return ForwardResponse(ok=False, error="not_leader")
        except ProposalError as e:
            return ForwardResponse(ok=False, error=str(e))

    def _handle_timeout_now(self, req: TimeoutNowRequest) -> TimeoutNowResponse:
        """The leader is handing over to us: start an election right now."""
        with self._state_lock:
            n = self._node
            if req.term >= n.current_term and self.node_id in self._members \
                    and n.state != RaftState.LEADER:
                logger.info(f"[{self.node_id}] {req.leader_id} is handing over leadership")
                self._become_candidate(transfer=True)
            return TimeoutNowResponse(term=n.current_term)

    # ──────────────────────────────────────────────
    # RPC CLIENT (send RPCs to peers)
    # ──────────────────────────────────────────────

    def _send_request_vote(self, address: str,
                           req: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Send a RequestVote RPC to one peer, return response or None."""
        return self._call(address, "RequestVote", req, decode_request_vote_resp)

    def _send_append_entries(self, address: str,
                             req: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send an AppendEntries RPC to one peer, return response or None."""
        return self._call(address, "AppendEntries", req, decode_append_entries_resp)

    def _send_install_snapshot(self, address: str,
                               req: InstallSnapshotRequest) -> Optional[InstallSnapshotResponse]:
        return self._call(address, "InstallSnapshot", req, decode_install_snapshot_resp,
                          timeout=SNAPSHOT_RPC_TIMEOUT)

    def _call(self, address: str, msg_type: str, request, decode_fn,
              timeout: float = RPC_TIMEOUT):
        """One request/response round trip on a fresh connection. None on any failure."""
        if address in self._blocked:
            return None
        try:
            host, port = address.rsplit(":", 1)
            # Connecting to a dead node must fail fast (Windows retries a
            # refused connection for ~2s); only the reply may take `timeout`.
            with socket.create_connection((host, int(port)),
                                          timeout=min(timeout, RPC_TIMEOUT)) as raw:
                raw.settimeout(timeout)
                sock = (self._ssl_client_context.wrap_socket(raw, server_hostname=host)
                        if self._ssl_client_context else raw)
                try:
                    sock.sendall(msg_type.encode() + b"\n" + encode(request))
                    with sock.makefile("rb") as reader:
                        line = reader.readline()
                finally:
                    if sock is not raw:
                        sock.close()
            if line:
                return decode_fn(line.decode())
        except Exception as e:
            logger.debug(f"[{self.node_id}] {msg_type} to {address} failed: {e}")
        return None

    # ──────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────

    def _restore_persistent_state(self):
        """Reload term, vote, log, snapshot boundary and membership."""
        saved = self._storage.load()
        n = self._node
        n.current_term    = saved.current_term
        n.voted_for       = saved.voted_for
        n.snapshot_index  = saved.snapshot_index
        n.snapshot_term   = saved.snapshot_term
        n.snapshot_config = saved.snapshot_config
        n.log             = list(saved.log)
        # Everything up to last_applied is already reflected in the state
        # machine; entries after it are re-applied once the leader tells
        # us they are committed (the state machine drops duplicates).
        n.commit_index = n.last_applied = max(saved.last_applied, saved.snapshot_index)
        if n.snapshot_config is None:
            # First start: remember the bootstrap membership, so a later
            # edit of cluster.json can't silently change who votes.
            n.snapshot_config = list(self._bootstrap)
            self._storage.save_snapshot(n.snapshot_index, n.snapshot_term,
                                        n.last_applied, n.snapshot_config)
        if n.current_term or n.log:
            logger.info(f"[{self.node_id}] Restored term {n.current_term}, "
                        f"{len(n.log)} log entries, snapshot at {n.snapshot_index}")
        self._refresh_config()

    def _spawn(self, target, *args, name: Optional[str] = None) -> threading.Thread:
        """Start a daemon thread that releases its storage resources when done."""
        def run():
            try:
                target(*args)
            except Exception:
                logger.exception(f"[{self.node_id}] Thread {threading.current_thread().name} crashed")
            finally:
                self._storage.close_thread_resources()

        thread = threading.Thread(target=run, daemon=True, name=name)
        thread.start()
        return thread


def _remove_quietly(path: str):
    try:
        os.remove(path)
    except OSError:
        pass
