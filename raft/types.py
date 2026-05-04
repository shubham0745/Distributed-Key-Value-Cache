"""
raft/types.py

Core data types for the Raft consensus algorithm.
Every concept here maps directly to the Raft paper.
"""
import time
from enum import Enum
from dataclasses import dataclass, field


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
    command:  str   # "SET key value" or "DELETE key"
    username: str   # which user's cache to apply to


@dataclass
class RaftNode:
    """
    Complete state of one Raft node.

    Persistent state (must survive crashes — Week 6 saves to MySQL):
        current_term  — latest term this node has seen
        voted_for     — candidate_id we voted for in current term
        log           — list of LogEntry

    Volatile state (rebuilt from log on restart):
        commit_index  — highest log index known to be committed
        last_applied  — highest log index applied to state machine

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

    def last_log_index(self) -> int:
        """Index of the last entry in our log (0 if empty)."""
        return len(self.log)

    def last_log_term(self) -> int:
        """Term of the last log entry (0 if log is empty)."""
        if self.log:
            return self.log[-1].term
        return 0

    def is_leader(self) -> bool:
        return self.state == RaftState.LEADER

    def is_follower(self) -> bool:
        return self.state == RaftState.FOLLOWER

    def is_candidate(self) -> bool:
        return self.state == RaftState.CANDIDATE