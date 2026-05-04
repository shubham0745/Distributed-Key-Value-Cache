"""
raft/node.py

The Raft consensus node — leader election implementation.

This is the hardest file in the entire project. Read every comment.

HOW LEADER ELECTION WORKS:
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
"""
import socket
import threading
import time
import random
import logging
from typing import Optional, Callable

from raft.types import RaftState, RaftNode, LogEntry
from raft.rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    encode,
    decode_request_vote_req, decode_request_vote_resp,
    decode_append_entries_req, decode_append_entries_resp,
)

logger = logging.getLogger(__name__)

# Timing constants (in seconds)
HEARTBEAT_INTERVAL    = 0.5      # Leader sends heartbeat every 500ms
ELECTION_TIMEOUT_MIN  = 1.5      # Follower waits at least 1.5s
ELECTION_TIMEOUT_MAX  = 3.0      # Follower waits at most 3.0s


class RaftEngine:
    """
    Core Raft implementation — handles leader election and heartbeats.
    Log replication is added in Week 5.

    Usage:
        engine = RaftEngine(
            node_id="node1",
            host="127.0.0.1",
            port=9001,
            peers=["127.0.0.1:9002", "127.0.0.1:9003"]
        )
        engine.start()   # starts election timer + RPC server
    """

    def __init__(self, node_id: str, host: str, port: int,
                 peers: list[str],
                 on_become_leader: Optional[Callable] = None,
                 on_become_follower: Optional[Callable] = None):
        self.node_id = node_id
        self.host    = host
        self.port    = port
        self.peers   = peers   # ["host:port", ...]

        # Callbacks — TCP server can react to state changes
        self.on_become_leader   = on_become_leader
        self.on_become_follower = on_become_follower

        # Core Raft state — protected by a single lock
        self._state_lock = threading.RLock()
        self._node = RaftNode(node_id=node_id, peers=peers)

        # Controls
        self._running = False
        self._election_timer: Optional[threading.Timer] = None

    # ──────────────────────────────────────────────
    # PUBLIC API
    # ──────────────────────────────────────────────

    def start(self):
        """Start the Raft engine — RPC server + election timer."""
        self._running = True
        # Start the RPC server in background thread
        rpc_thread = threading.Thread(
            target=self._run_rpc_server,
            daemon=True,
            name=f"raft-rpc-{self.node_id}"
        )
        rpc_thread.start()
        time.sleep(0.1)  # Let RPC server bind
        # Start election timer
        self._reset_election_timer()
        logger.info(f"[{self.node_id}] Raft engine started on port {self.port}")

    def stop(self):
        """Stop the Raft engine."""
        self._running = False
        if self._election_timer:
            self._election_timer.cancel()

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
            return None  # Week 5: followers track leader_id

    def is_leader(self) -> bool:
        with self._state_lock:
            return self._node.state == RaftState.LEADER

    # ──────────────────────────────────────────────
    # ELECTION TIMER
    # ──────────────────────────────────────────────

    def _reset_election_timer(self):
        """
        Reset the election timeout with a NEW random duration.
        Called:
          - On startup
          - When we receive a valid heartbeat
          - When we grant a vote
          - When we become a follower
        """
        if self._election_timer:
            self._election_timer.cancel()

        if not self._running:
            return

        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self._election_timer = threading.Timer(timeout, self._start_election)
        self._election_timer.daemon = True
        self._election_timer.start()

    def _start_election(self):
        """
        Election timeout fired — no heartbeat received in time.
        Transition: FOLLOWER → CANDIDATE and request votes.
        """
        with self._state_lock:
            # Only followers and candidates can start elections
            if not self._running:
                return
            if self._node.state == RaftState.LEADER:
                return

            # Increment term and vote for self
            self._node.current_term += 1
            self._node.state     = RaftState.CANDIDATE
            self._node.voted_for = self.node_id
            term = self._node.current_term
            last_log_index = self._node.last_log_index()
            last_log_term  = self._node.last_log_term()

        logger.info(f"[{self.node_id}] Starting election for term {term}")

        # We already have our own vote — need majority of 3 = 2 total
        votes_received = 1
        votes_needed   = (len(self.peers) + 1) // 2 + 1  # majority

        # Send RequestVote to all peers in parallel
        vote_lock = threading.Lock()
        threads = []

        def request_vote_from(peer: str):
            nonlocal votes_received
            response = self._send_request_vote(peer, RequestVoteRequest(
                term=term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            ))
            if response is None:
                return

            with self._state_lock:
                # If we see a higher term, immediately step down
                if response.term > self._node.current_term:
                    self._become_follower(response.term)
                    return

            if response.vote_granted:
                with vote_lock:
                    votes_received += 1
                    logger.info(f"[{self.node_id}] Got vote from {peer} "
                                f"({votes_received}/{votes_needed} needed)")

        for peer in self.peers:
            t = threading.Thread(target=request_vote_from, args=(peer,), daemon=True)
            t.start()
            threads.append(t)

        # Wait for all vote requests to complete (with timeout)
        for t in threads:
            t.join(timeout=1.0)

        # Check if we won
        with self._state_lock:
            if (self._node.state == RaftState.CANDIDATE and
                    self._node.current_term == term and
                    votes_received >= votes_needed):
                self._become_leader()
            elif self._node.state == RaftState.CANDIDATE:
                # Lost or split vote — reset timer and try again later
                logger.info(f"[{self.node_id}] Lost election for term {term} "
                            f"(got {votes_received}/{votes_needed} votes)")
                self._reset_election_timer()

    # ──────────────────────────────────────────────
    # STATE TRANSITIONS
    # ──────────────────────────────────────────────

    def _become_leader(self):
        """
        We won the election. Transition to LEADER.
        Must be called while holding _state_lock.
        """
        self._node.state = RaftState.LEADER
        # Initialize leader tracking for each peer
        next_idx = self._node.last_log_index() + 1
        for peer in self.peers:
            self._node.next_index[peer]  = next_idx
            self._node.match_index[peer] = 0

        logger.info(f"[{self.node_id}] *** BECAME LEADER for term "
                    f"{self._node.current_term} ***")

        if self.on_become_leader:
            self.on_become_leader()

        # Cancel election timer — leaders don't need it
        if self._election_timer:
            self._election_timer.cancel()

        # Start sending heartbeats
        threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{self.node_id}"
        ).start()

    def _become_follower(self, term: int):
        """
        Step down to follower — saw a higher term.
        Must be called while holding _state_lock.
        """
        was_leader = self._node.state == RaftState.LEADER
        self._node.state       = RaftState.FOLLOWER
        self._node.current_term = term
        self._node.voted_for   = None
        logger.info(f"[{self.node_id}] Became follower for term {term}")

        if was_leader and self.on_become_follower:
            self.on_become_follower()

        self._reset_election_timer()

    # ──────────────────────────────────────────────
    # HEARTBEAT LOOP (Leader only)
    # ──────────────────────────────────────────────

    def _heartbeat_loop(self):
        """
        Leader sends AppendEntries (empty = heartbeat) to all peers
        every HEARTBEAT_INTERVAL seconds.

        This prevents followers from timing out and starting elections.
        As long as heartbeats arrive, there is peace in the cluster.
        """
        while self._running:
            with self._state_lock:
                if self._node.state != RaftState.LEADER:
                    break
                term      = self._node.current_term
                commit_idx = self._node.commit_index

            for peer in self.peers:
                threading.Thread(
                    target=self._send_heartbeat,
                    args=(peer, term, commit_idx),
                    daemon=True
                ).start()

            time.sleep(HEARTBEAT_INTERVAL)

    def _send_heartbeat(self, peer: str, term: int, commit_index: int):
        """Send an empty AppendEntries (heartbeat) to one peer."""
        with self._state_lock:
            prev_log_index = self._node.last_log_index()
            prev_log_term  = self._node.last_log_term()

        request = AppendEntriesRequest(
            term=term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=[],   # empty = heartbeat only
            leader_commit=commit_index,
        )
        response = self._send_append_entries(peer, request)
        if response is None:
            return

        with self._state_lock:
            if response.term > self._node.current_term:
                self._become_follower(response.term)

    # ──────────────────────────────────────────────
    # RPC SERVER (receives incoming RPCs from peers)
    # ──────────────────────────────────────────────

    def _run_rpc_server(self):
        """
        Listen for incoming Raft RPCs from other nodes.
        Each connection is handled in its own thread.
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(10)
        server_sock.settimeout(1.0)

        while self._running:
            try:
                conn, addr = server_sock.accept()
                threading.Thread(
                    target=self._handle_rpc,
                    args=(conn,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break

        server_sock.close()

    def _handle_rpc(self, conn: socket.socket):
        """
        Handle one incoming RPC connection.
        Reads the message type, dispatches to the right handler,
        and sends back the response.

        Protocol:
            Line 1: message type ("RequestVote" or "AppendEntries")
            Line 2: JSON payload
            Response: JSON payload + newline
        """
        try:
            conn.settimeout(2.0)
            # Read message type
            msg_type = self._recv_line(conn)
            if msg_type is None:
                return
            # Read JSON payload
            payload = self._recv_line(conn)
            if payload is None:
                return

            if msg_type == "RequestVote":
                req = decode_request_vote_req(payload)
                resp = self._handle_request_vote(req)
                conn.sendall(encode(resp))

            elif msg_type == "AppendEntries":
                req = decode_append_entries_req(payload)
                resp = self._handle_append_entries(req)
                conn.sendall(encode(resp))

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

            # Grant the vote
            self._node.voted_for = req.candidate_id
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

        For Week 4 (leader election only):
          - If req.term >= our term → accept, reset election timer
          - If req.term < our term  → reject
        Log replication logic is added in Week 5.
        """
        with self._state_lock:
            # Reject stale leaders
            if req.term < self._node.current_term:
                return AppendEntriesResponse(
                    term=self._node.current_term,
                    success=False
                )

            # Valid leader — update term and become follower if needed
            if req.term > self._node.current_term:
                self._node.current_term = req.term
                self._node.voted_for    = None

            self._node.state = RaftState.FOLLOWER
            self._node.last_heartbeat = time.time()
            self._reset_election_timer()   # reset — we heard from leader

            logger.debug(f"[{self.node_id}] Heartbeat from {req.leader_id} "
                         f"term={req.term}")

            return AppendEntriesResponse(
                term=self._node.current_term,
                success=True
            )

    # ──────────────────────────────────────────────
    # RPC CLIENT (send RPCs to peers)
    # ──────────────────────────────────────────────

    def _send_request_vote(self, peer: str,
                           req: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Send a RequestVote RPC to one peer, return response or None."""
        try:
            host, port = peer.split(":")
            with socket.create_connection((host, int(port)), timeout=1.0) as sock:
                sock.sendall(b"RequestVote\n")
                sock.sendall(encode(req))
                data = self._recv_line_from(sock)
                if data:
                    return decode_request_vote_resp(data)
        except Exception as e:
            logger.debug(f"[{self.node_id}] RequestVote to {peer} failed: {e}")
        return None

    def _send_append_entries(self, peer: str,
                             req: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send an AppendEntries RPC to one peer, return response or None."""
        try:
            host, port = peer.split(":")
            with socket.create_connection((host, int(port)), timeout=1.0) as sock:
                sock.sendall(b"AppendEntries\n")
                sock.sendall(encode(req))
                data = self._recv_line_from(sock)
                if data:
                    return decode_append_entries_resp(data)
        except Exception as e:
            logger.debug(f"[{self.node_id}] AppendEntries to {peer} failed: {e}")
        return None

    # ──────────────────────────────────────────────
    # SOCKET HELPERS
    # ──────────────────────────────────────────────

    def _recv_line(self, sock: socket.socket) -> Optional[str]:
        data = b""
        while not data.endswith(b"\n"):
            chunk = sock.recv(1)
            if not chunk:
                return None
            data += chunk
        return data.decode().strip()

    def _recv_line_from(self, sock: socket.socket) -> Optional[str]:
        return self._recv_line(sock)