"""
raft/types.py

Core data types for the Raft consensus algorithm.
Every concept here maps directly to the Raft paper.
"""
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


# A new leader appends one of these as soon as it wins. It changes no data,
# but committing it also commits every entry left over from older terms
# (a leader may only count replicas for entries of its OWN term — §5.4.2).
NOOP = "NOOP"


class RaftState(Enum):
    """
    Every Raft node is always in exactly one of these three states.

    FOLLOWER  — default state. Waits for heartbeats from the leader.
                If no heartbeat arrives within the election timeout,
                it becomes a CANDIDATE and starts an election.

    CANDIDATE — believes there is no leader. Votes for itself and
                sends RequestVote RPCs to all other nodes.
                Becomes LEADER if it wins majority, else goes back
                to FOLLOWER if it sees a higher term.

    LEADER    — the ONE node that handles all writes.
                Sends AppendEntries RPCs (heartbeats + log entries)
                to all followers to keep them in sync.
    """
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


@dataclass
class LogEntry:
    """
    One entry in the Raft log.

    The log is the SOURCE OF TRUTH in Raft. Every write (SET/DELETE)
    becomes a log entry. The leader replicates entries to followers.
    Once a majority of nodes have the entry, it is "committed" and
    applied to the actual cache.

    Fields:
        term     — which election term this entry was created in
        index    — position in the log (1-based)
        command  — the actual operation, e.g. "SET name shubham"
        username — which user's cache this affects
    """
    term:     int
    index:    int
    command:  str   # "SIGNUP <hash>", "SET key value", "DELETE key" or NOOP
    username: str   # which user's cache to apply to


@dataclass
class RaftNode:
    """
    Complete state of one Raft node.

    Persistent state (must survive crashes — saved through a RaftStorage):
        current_term  — latest term this node has seen
        voted_for     — candidate_id we voted for in current term
        log           — list of LogEntry (only entries AFTER the snapshot)

    Snapshot (log compaction):
        snapshot_index — last log index folded into the snapshot
        snapshot_term  — term of that entry
        Entries 1..snapshot_index are no longer in `log`; their effect
        already lives in the state machine (the cache / MySQL).

    Volatile state (rebuilt from log on restart):
        commit_index  — highest log index known to be committed
        last_applied  — highest log index applied to state machine
        leader_id     — who we believe the leader is (for client redirects)

    Leader-only volatile state (reset after each election):
        next_index    — for each follower, next log index to send
        match_index   — for each follower, highest index replicated
    """
    # Identity
    node_id:  str         # e.g. "node1"
    peers:    list        # list of peer addresses e.g. ["127.0.0.1:9001"]

    # Persistent state
    current_term: int = 0
    voted_for:    str = None      # node_id of who we voted for
    log:          list = field(default_factory=list)  # list[LogEntry]

    # Volatile state
    state:        RaftState = RaftState.FOLLOWER
    commit_index: int = 0
    last_applied: int = 0

    # Leader-only (keyed by peer address)
    next_index:   dict = field(default_factory=dict)
    match_index:  dict = field(default_factory=dict)

    # Timing
    last_heartbeat: float = field(default_factory=time.time)

    # Who the leader is (None while unknown / during an election)
    leader_id: Optional[str] = None

    # Snapshot boundary
    snapshot_index: int = 0
    snapshot_term:  int = 0

    def last_log_index(self) -> int:
        """Index of the last entry in our log (0 if empty)."""
        return self.snapshot_index + len(self.log)

    def last_log_term(self) -> int:
        """Term of the last log entry (0 if log is empty)."""
        if self.log:
            return self.log[-1].term
        return self.snapshot_term

    def term_at(self, index: int) -> Optional[int]:
        """
        Term of the entry at `index`.
        Returns None when we don't have it: it is past the end of our
        log, or already compacted away inside the snapshot.
        """
        if index == self.snapshot_index:
            return self.snapshot_term           # 0 when there is no snapshot
        if index < self.snapshot_index or index > self.last_log_index():
            return None
        return self.log[index - self.snapshot_index - 1].term

    def entry_at(self, index: int) -> Optional[LogEntry]:
        if index <= self.snapshot_index or index > self.last_log_index():
            return None
        return self.log[index - self.snapshot_index - 1]

    def entries_from(self, index: int, limit: int) -> list:
        """Up to `limit` entries starting at `index` (must be > snapshot_index)."""
        start = index - self.snapshot_index - 1
        return self.log[start:start + limit]

    def truncate_from(self, index: int) -> None:
        """Delete the entry at `index` and everything after it."""
        del self.log[index - self.snapshot_index - 1:]

    def compact_through(self, index: int, term: int) -> None:
        """
        Fold entries 1..index into the snapshot.
        If our entry at `index` has the same term we keep the entries
        after it; otherwise our log disagrees with the snapshot and is
        thrown away entirely.
        """
        if self.term_at(index) == term:
            self.log = self.log[index - self.snapshot_index:]
        else:
            self.log = []
        self.snapshot_index = index
        self.snapshot_term  = term

    def is_leader(self) -> bool:
        return self.state == RaftState.LEADER

    def is_follower(self) -> bool:
        return self.state == RaftState.FOLLOWER

    def is_candidate(self) -> bool:
        return self.state == RaftState.CANDIDATE
