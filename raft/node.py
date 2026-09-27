"""
raft/node.py

The Raft consensus node — leader election, log replication,
persistence and snapshots.

This is the hardest file in the entire project. Read every comment.

THE BIG PICTURE
Every write (SIGNUP / SET / DELETE) becomes a LogEntry. The leader puts
it in its log, copies it to the followers, and once a MAJORITY of nodes
store it the entry is "committed". Every node then hands committed
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
A follower that needs dropped entries gets the whole state machine
instead (InstallSnapshot).

LOCKING RULES
  _state_lock — guards all Raft state (_node). Held only briefly; no
                network I/O while holding it (storage writes are allowed).
  _apply_lock — held while changing the state machine (apply / snapshot
                install / snapshot dump). Always taken BEFORE _state_lock.
  Callbacks (on_become_leader / on_become_follower) run with _state_lock
  held: keep them short, and never wait on another thread that needs
  this engine.
"""
import socket
import threading
import time
import random
import logging
from typing import Any, Callable, Optional

from raft.types import RaftState, RaftNode, LogEntry, NOOP
from raft.rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    encode,
    decode_request_vote_req, decode_request_vote_resp,
    decode_append_entries_req, decode_append_entries_resp,
    decode_install_snapshot_req, decode_install_snapshot_resp,
)
from raft.storage import RaftStorage, MemoryRaftStorage

logger = logging.getLogger(__name__)

# Timing constants (in seconds)
HEARTBEAT_INTERVAL    = 0.5      # Leader sends heartbeat every 500ms
ELECTION_TIMEOUT_MIN  = 1.5      # Follower waits at least 1.5s
ELECTION_TIMEOUT_MAX  = 3.0      # Follower waits at most 3.0s
RPC_TIMEOUT           = 1.0      # Give up on a peer's reply after 1s
SNAPSHOT_RPC_TIMEOUT  = 10.0     # Snapshots can be large

MAX_ENTRIES_PER_RPC        = 100    # Batch size when a follower is behind
DEFAULT_SNAPSHOT_THRESHOLD = 1000   # Compact after this many applied entries

_PENDING = object()   # marker: a proposal is still waiting for its result
_STOPPED = object()   # marker: the engine stopped while applying


class NotLeaderError(Exception):
    """Raised by propose() on a node that is not the leader."""

    def __init__(self, leader_id: Optional[str]):
        self.leader_id = leader_id
        super().__init__(f"not the leader (current leader: {leader_id or 'unknown'})")


class ProposalError(Exception):
    """A write could not be confirmed (no majority in time, or overwritten)."""


class RaftEngine:
    """
    Core Raft implementation.

    Usage:
        engine = RaftEngine(
            node_id="node1",
            host="127.0.0.1",
            port=9001,
            peers=["127.0.0.1:9002", "127.0.0.1:9003"],
            apply_fn=state_machine.apply,
        )
        engine.start()                              # RPC server + timers
        result = engine.propose("SET k v", "shubham")   # leader only

    port=None means "don't listen for RPCs" — only valid with no peers
    (the single-node server uses this).
    """

    def __init__(self, node_id: str, host: str, port: Optional[int],
                 peers: list[str],
                 on_become_leader: Optional[Callable] = None,
                 on_become_follower: Optional[Callable] = None,
                 apply_fn: Optional[Callable[[LogEntry], Any]] = None,
                 snapshot_fn: Optional[Callable[[], Any]] = None,
                 restore_fn: Optional[Callable[[Any], None]] = None,
                 storage: Optional[RaftStorage] = None,
                 heartbeat_interval: float = HEARTBEAT_INTERVAL,
                 election_timeout: tuple[float, float] = (ELECTION_TIMEOUT_MIN,
                                                          ELECTION_TIMEOUT_MAX),
                 snapshot_threshold: int = DEFAULT_SNAPSHOT_THRESHOLD):
        self.node_id = node_id
        self.host    = host
        self.port    = port
        self.peers   = list(peers)   # ["host:port", ...]

        # Callbacks — TCP server can react to state changes
        self.on_become_leader   = on_become_leader
        self.on_become_follower = on_become_follower

        # The state machine (our cache) — see server/state_machine.py
        self._apply_fn    = apply_fn
        self._snapshot_fn = snapshot_fn
        self._restore_fn  = restore_fn

        self._storage = storage or MemoryRaftStorage()
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout   = election_timeout
        self.snapshot_threshold = snapshot_threshold

        # Core Raft state — protected by a single lock
        self._state_lock = threading.RLock()
        self._cond = threading.Condition(self._state_lock)   # "something changed"
        self._apply_lock = threading.Lock()
        self._node = RaftNode(node_id=node_id, peers=self.peers)
        self._restore_persistent_state()

        # Controls
        self._running = False
        self._election_timer: Optional[threading.Timer] = None
        self._timer_generation = 0
        self._server_socket: Optional[socket.socket] = None
        self._replicate_events: dict[str, threading.Event] = {}
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
        if not self.peers:
            # Alone in the cluster: we are trivially the majority.
            self._start_election()
        else:
            with self._state_lock:
                self._reset_election_timer()

    def stop(self):
        """Stop the Raft engine."""
        with self._state_lock:
            self._running = False
            self._timer_generation += 1
            if self._election_timer:
                self._election_timer.cancel()
            self._cond.notify_all()
        for event in list(self._replicate_events.values()):
            event.set()
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
                "peers": len(self.peers),
            }

    def propose(self, command: str, username: str, timeout: float = 5.0) -> Any:
        """
        Replicate one command and wait until it is committed AND applied
        on this node. Returns whatever apply_fn returned for it.

        Raises:
            NotLeaderError — this node isn't (or stopped being) the leader
            ProposalError  — no majority within `timeout`, or a new leader
                             replaced the entry. The write may or may not
                             have happened; SET/DELETE are safe to retry.
        """
        with self._state_lock:
            n = self._node
            if not self._running:
                raise NotLeaderError(None)
            if n.state != RaftState.LEADER:
                raise NotLeaderError(n.leader_id)
            term = n.current_term
            entry = LogEntry(term=term, index=n.last_log_index() + 1,
                             command=command, username=username)
            n.log.append(entry)
            self._storage.append([entry])
            self._waiters[entry.index] = _PENDING
            self._advance_commit_index()      # a lone node commits right away
            events = list(self._replicate_events.values())

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
          - When we start an election (in case it ends in a split vote)

        Every reset bumps a generation number. A timer that already fired
        but hasn't grabbed the lock yet sees a newer generation and gives
        up — cancel() alone can't stop a timer that is already running.
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
        Transition: FOLLOWER → CANDIDATE and request votes.
        Votes are counted as they arrive; we become leader the moment
        we reach a majority instead of waiting for slow peers.
        """
        with self._state_lock:
            if not self._running:
                return
            if generation is not None and generation != self._timer_generation:
                return      # stale timer — a heartbeat arrived just in time
            # Only followers and candidates can start elections
            if self._node.state == RaftState.LEADER:
                return

            # Increment term and vote for self
            n = self._node
            n.current_term += 1
            n.state        = RaftState.CANDIDATE
            n.voted_for    = self.node_id
            n.leader_id    = None
            self._persist_term_and_vote()
            votes = {self.node_id}

            logger.info(f"[{self.node_id}] Starting election for term {n.current_term}")

            if len(votes) >= self._majority():
                self._become_leader()           # single-node cluster
                return

            self._reset_election_timer()        # retry if this one stalls
            request = RequestVoteRequest(
                term=n.current_term,
                candidate_id=self.node_id,
                last_log_index=n.last_log_index(),
                last_log_term=n.last_log_term(),
            )

        # Send RequestVote to all peers in parallel
        for peer in self.peers:
            self._spawn(self._request_vote_from, peer, request, votes)

    def _request_vote_from(self, peer: str, request: RequestVoteRequest, votes: set):
        response = self._send_request_vote(peer, request)
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
            votes.add(peer)
            logger.info(f"[{self.node_id}] Got vote from {peer} "
                        f"({len(votes)}/{self._majority()} needed)")
            if len(votes) >= self._majority():
                self._become_leader()

    def _majority(self) -> int:
        """Votes / replicas needed: 2 of 3, 3 of 5, 1 of 1..."""
        return (len(self.peers) + 1) // 2 + 1

    # ──────────────────────────────────────────────
    # STATE TRANSITIONS  (all called with _state_lock held)
    # ──────────────────────────────────────────────

    def _become_leader(self):
        """We won the election. Transition to LEADER."""
        n = self._node
        n.state = RaftState.LEADER
        n.leader_id = self.node_id

        # Cancel election timer — leaders don't need it
        self._timer_generation += 1
        if self._election_timer:
            self._election_timer.cancel()

        # Initialize leader tracking for each peer: optimistically assume
        # they have everything we have; the consistency check corrects us.
        next_idx = n.last_log_index() + 1
        for peer in self.peers:
            n.next_index[peer]  = next_idx
            n.match_index[peer] = 0

        # Commit a no-op from our own term (see NOOP in raft/types.py)
        noop = LogEntry(term=n.current_term, index=next_idx, command=NOOP, username="")
        n.log.append(noop)
        self._storage.append([noop])

        logger.info(f"[{self.node_id}] *** BECAME LEADER for term "
                    f"{n.current_term} ***")

        if self.on_become_leader:
            self.on_become_leader()

        self._advance_commit_index()

        # One replicator per follower — doubles as the heartbeat
        self._replicate_events = {}
        for peer in self.peers:
            event = threading.Event()
            self._replicate_events[peer] = event
            self._spawn(self._replicate_loop, peer, n.current_term, event,
                        name=f"replicate-{self.node_id}->{peer}")

    def _become_follower(self, term: int):
        """
        Step down to follower (saw a higher term, or a valid leader).
        voted_for is only cleared when the term really changes —
        clearing it inside the same term would allow voting twice.
        """
        n = self._node
        was_leader = n.state == RaftState.LEADER
        if term > n.current_term:
            n.current_term = term
            n.voted_for    = None
            n.leader_id    = None
            self._persist_term_and_vote()
        if n.state != RaftState.FOLLOWER:
            logger.info(f"[{self.node_id}] Became follower for term {n.current_term}")
        n.state = RaftState.FOLLOWER

        if was_leader and self.on_become_follower:
            self.on_become_follower()

        self._reset_election_timer()
        self._cond.notify_all()     # wake proposals waiting on our leadership

    def _accept_leader(self, term: int, leader_id: str):
        """A valid AppendEntries / InstallSnapshot arrived (term >= ours)."""
        n = self._node
        if term > n.current_term or n.state != RaftState.FOLLOWER:
            self._become_follower(term)
        else:
            self._reset_election_timer()   # reset — we heard from leader
        if n.leader_id != leader_id:
            logger.info(f"[{self.node_id}] Following leader {leader_id} (term {term})")
            n.leader_id = leader_id
        n.last_heartbeat = time.time()

    def _persist_term_and_vote(self):
        self._storage.save_term_and_vote(self._node.current_term, self._node.voted_for)

    # ──────────────────────────────────────────────
    # REPLICATION (Leader only)
    # ──────────────────────────────────────────────

    def _replicate_loop(self, peer: str, term: int, wakeup: threading.Event):
        """
        Keeps ONE follower in sync for as long as we lead `term`.

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
                if (not self._running or n.state != RaftState.LEADER or
                        n.current_term != term):
                    return
                next_idx = n.next_index[peer]
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
                    )

            if request is None:
                reachable = self._send_snapshot_to(peer, term)
            else:
                reachable = self._send_entries_to(peer, term, request)

            with self._state_lock:
                behind = (reachable and self._node.state == RaftState.LEADER and
                          self._node.next_index.get(peer, 0) <= self._node.last_log_index())
            if not behind:
                wakeup.wait(self.heartbeat_interval)

    def _send_entries_to(self, peer: str, term: int,
                         request: AppendEntriesRequest) -> bool:
        """Send one AppendEntries and process the reply. False if unreachable."""
        response = self._send_append_entries(peer, request)
        if response is None:
            return False

        with self._state_lock:
            n = self._node
            if response.term > n.current_term:
                self._become_follower(response.term)
                return True
            if n.state != RaftState.LEADER or n.current_term != term:
                return True

            if response.success:
                match = max(request.prev_log_index + len(request.entries),
                            response.match_index)
                n.match_index[peer] = max(n.match_index[peer], match)
                n.next_index[peer]  = n.match_index[peer] + 1
                self._advance_commit_index()
            else:
                # Logs disagree at prev_log_index — back up and try again.
                hint = response.conflict_index or request.prev_log_index
                n.next_index[peer] = max(1, min(hint, request.prev_log_index))
        return True

    def _send_snapshot_to(self, peer: str, term: int) -> bool:
        """The follower needs entries we compacted: send the state machine."""
        with self._apply_lock:              # freeze the state machine
            with self._state_lock:
                n = self._node
                if n.state != RaftState.LEADER or n.current_term != term:
                    return True
                index = n.last_applied
                last_term = n.term_at(index)
            data = self._snapshot_fn() if self._snapshot_fn else None

        logger.info(f"[{self.node_id}] Sending snapshot (up to entry {index}) to {peer}")
        response = self._send_install_snapshot(peer, InstallSnapshotRequest(
            term=term,
            leader_id=self.node_id,
            last_included_index=index,
            last_included_term=last_term,
            data=data,
        ))
        if response is None:
            return False

        with self._state_lock:
            n = self._node
            if response.term > n.current_term:
                self._become_follower(response.term)
            elif n.state == RaftState.LEADER and n.current_term == term:
                n.match_index[peer] = max(n.match_index[peer], index)
                n.next_index[peer]  = n.match_index[peer] + 1
                self._advance_commit_index()
        return True

    def _advance_commit_index(self):
        """
        Leader: commit the highest index stored on a majority.
        Only entries from OUR term are counted (Raft §5.4.2) — older
        entries become committed along with them.
        Called with _state_lock held.
        """
        n = self._node
        if n.state != RaftState.LEADER:
            return
        for index in range(n.last_log_index(), n.commit_index, -1):
            if n.term_at(index) != n.current_term:
                break
            replicas = 1 + sum(1 for p in self.peers if n.match_index.get(p, 0) >= index)
            if replicas >= self._majority():
                n.commit_index = index
                self._cond.notify_all()     # wake the applier
                # Tell followers now rather than at the next heartbeat,
                # so they apply (and can serve) the entry sooner.
                for event in self._replicate_events.values():
                    event.set()
                break

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
                    self._node.last_applied = max(self._node.last_applied, index)
                    if index in self._waiters:
                        self._waiters[index] = (entry.term, result)
                    self._cond.notify_all()
                    self._maybe_snapshot()

    def _apply_with_retry(self, entry: LogEntry) -> Any:
        """
        Apply one entry. An exception here means infrastructure trouble
        (e.g. MySQL is down), not a bad command — skipping the entry would
        make this node's data diverge, so we retry until it works.
        """
        if entry.command == NOOP or self._apply_fn is None:
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
        n.compact_through(index, term)
        self._storage.save_snapshot(index, term, n.last_applied)
        logger.info(f"[{self.node_id}] Compacted log through entry {index}")

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
            Line 1: message type ("RequestVote", "AppendEntries" or "InstallSnapshot")
            Line 2: JSON payload
            Response: JSON payload + newline
        """
        handlers = {
            "RequestVote":     (decode_request_vote_req,     self._handle_request_vote),
            "AppendEntries":   (decode_append_entries_req,   self._handle_append_entries),
            "InstallSnapshot": (decode_install_snapshot_req, self._handle_install_snapshot),
        }
        try:
            conn.settimeout(SNAPSHOT_RPC_TIMEOUT)
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
        """
        with self._state_lock:
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

            # Check log up-to-date-ness
            # Candidate must be at least as up-to-date as us
            our_last_term  = self._node.last_log_term()
            our_last_index = self._node.last_log_index()

            log_ok = (
                req.last_log_term > our_last_term or
                (req.last_log_term == our_last_term and
                 req.last_log_index >= our_last_index)
            )

            if not log_ok:
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

            self._accept_leader(req.term, req.leader_id)

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
                    n.truncate_from(entry.index)
                    self._storage.truncate_from(entry.index)
                    new_entries = entries[i:]
                    break
            if new_entries:
                n.log.extend(new_entries)
                self._storage.append(new_entries)

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
        """Replace our state machine with the leader's snapshot."""
        with self._state_lock:
            if req.term < self._node.current_term:
                return InstallSnapshotResponse(term=self._node.current_term)
            self._accept_leader(req.term, req.leader_id)
            if req.last_included_index <= self._node.last_applied:
                return InstallSnapshotResponse(term=self._node.current_term)

        with self._apply_lock:
            with self._state_lock:
                if req.last_included_index <= self._node.last_applied:
                    return InstallSnapshotResponse(term=self._node.current_term)
            logger.info(f"[{self.node_id}] Installing snapshot up to entry "
                        f"{req.last_included_index} from {req.leader_id}")
            if self._restore_fn:
                self._restore_fn(req.data)

            with self._state_lock:
                n = self._node
                keep_tail = n.term_at(req.last_included_index) == req.last_included_term
                if not keep_tail:
                    self._storage.truncate_from(n.snapshot_index + 1)
                n.compact_through(req.last_included_index, req.last_included_term)
                n.commit_index = max(n.commit_index, req.last_included_index)
                n.last_applied = max(n.last_applied, req.last_included_index)
                self._storage.save_snapshot(n.snapshot_index, n.snapshot_term, n.last_applied)
                self._cond.notify_all()
                return InstallSnapshotResponse(term=n.current_term)

    # ──────────────────────────────────────────────
    # RPC CLIENT (send RPCs to peers)
    # ──────────────────────────────────────────────

    def _send_request_vote(self, peer: str,
                           req: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Send a RequestVote RPC to one peer, return response or None."""
        return self._call(peer, "RequestVote", req, decode_request_vote_resp)

    def _send_append_entries(self, peer: str,
                             req: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send an AppendEntries RPC to one peer, return response or None."""
        return self._call(peer, "AppendEntries", req, decode_append_entries_resp)

    def _send_install_snapshot(self, peer: str,
                               req: InstallSnapshotRequest) -> Optional[InstallSnapshotResponse]:
        return self._call(peer, "InstallSnapshot", req, decode_install_snapshot_resp,
                          timeout=SNAPSHOT_RPC_TIMEOUT)

    def _call(self, peer: str, msg_type: str, request, decode_fn,
              timeout: float = RPC_TIMEOUT):
        """One request/response round trip on a fresh connection. None on any failure."""
        try:
            host, port = peer.rsplit(":", 1)
            with socket.create_connection((host, int(port)), timeout=timeout) as sock:
                sock.sendall(msg_type.encode() + b"\n" + encode(request))
                with sock.makefile("rb") as reader:
                    line = reader.readline()
            if line:
                return decode_fn(line.decode())
        except Exception as e:
            logger.debug(f"[{self.node_id}] {msg_type} to {peer} failed: {e}")
        return None

    # ──────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────

    def _restore_persistent_state(self):
        """Reload term, vote, log and snapshot boundary saved by a previous run."""
        saved = self._storage.load()
        n = self._node
        n.current_term   = saved.current_term
        n.voted_for      = saved.voted_for
        n.snapshot_index = saved.snapshot_index
        n.snapshot_term  = saved.snapshot_term
        n.log            = list(saved.log)
        # Everything up to last_applied is already reflected in the state
        # machine; entries after it are re-applied once the leader tells
        # us they are committed (SET/DELETE/SIGNUP are safe to repeat).
        n.commit_index = n.last_applied = max(saved.last_applied, saved.snapshot_index)
        if n.current_term or n.log:
            logger.info(f"[{self.node_id}] Restored term {n.current_term}, "
                        f"{len(n.log)} log entries, snapshot at {n.snapshot_index}")

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
