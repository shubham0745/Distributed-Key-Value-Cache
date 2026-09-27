"""
raft/rpc.py

RPC (Remote Procedure Call) message definitions for Raft.

The core Raft RPCs:
  1. RequestVote     — sent by CANDIDATE to gather votes (also used for
                       the Pre-Vote round, with pre_vote=True)
  2. AppendEntries   — sent by LEADER for heartbeats AND log replication
  3. InstallSnapshot — sent by LEADER, in chunks, to a follower so far
                       behind that the entries it needs were compacted

Extensions:
  4. ReadIndex       — follower asks the leader "what must I have applied
                       before I can answer a read linearizably?"
  5. Forward         — follower hands a client's write to the leader
  6. TimeoutNow      — leader asks a caught-up follower to take over now
                       (leadership transfer on a graceful shutdown)

We serialize these as JSON strings over TCP sockets between nodes.
"""
import json
from dataclasses import dataclass, asdict
from typing import Any, Optional

from raft.types import LogEntry


@dataclass
class RequestVoteRequest:
    """
    Sent by a CANDIDATE to every other node asking for their vote.

    Fields:
        term           — candidate's current term (for a pre-vote: the
                         term it WOULD use, i.e. its term + 1)
        candidate_id   — who is asking for the vote
        last_log_index — index of candidate's last log entry
        last_log_term  — term of candidate's last log entry
        pre_vote       — only asking "would you vote for me?"; nobody
                         changes any state
        transfer       — the old leader asked for this election, so
                         followers must not ignore it (see TimeoutNow)

    A node grants its vote if:
      1. candidate's term >= our term
      2. we haven't voted for anyone else this term
      3. candidate's log is at least as up-to-date as ours
         (last_log_term > ours, OR same term but longer log)
    """
    term:           int
    candidate_id:   str
    last_log_index: int
    last_log_term:  int
    pre_vote:       bool = False
    transfer:       bool = False


@dataclass
class RequestVoteResponse:
    """
    Response to a RequestVote RPC.

    Fields:
        term         — responder's current term (candidate updates if higher)
        vote_granted — True if vote was given, False otherwise
    """
    term:         int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    """
    Sent by LEADER to followers for two purposes:
      1. HEARTBEAT — empty entries list, just to say "I'm alive"
      2. LOG REPLICATION — entries list has new commands to replicate

    Fields:
        term          — leader's current term
        leader_id     — so followers can redirect clients
        prev_log_index— index of log entry immediately before new ones
        prev_log_term — term of prev_log_index entry
        entries       — list of new log entries (empty for heartbeat)
        leader_commit — leader's commit_index
        leader_address— leader's Raft "host:port", so followers can
                        forward writes and reads to it
    """
    term:           int
    leader_id:      str
    prev_log_index: int
    prev_log_term:  int
    entries:        list   # list of LogEntry (dicts on the wire)
    leader_commit:  int
    leader_address: str = ""


@dataclass
class AppendEntriesResponse:
    """
    Response to an AppendEntries RPC.

    Fields:
        term           — follower's current term (leader steps down if higher)
        success        — True if follower accepted the entries
        match_index    — on success: the follower's log now matches the
                         leader's up to this index
        conflict_index — on failure: where the leader should retry from.
                         Lets the leader skip back a whole term at once
                         instead of one entry per round trip.
    """
    term:    int
    success: bool
    match_index:    int = 0
    conflict_index: int = 0


@dataclass
class InstallSnapshotRequest:
    """
    Sent by LEADER when a follower needs entries the leader has already
    compacted. Carries the state machine instead of the log, streamed in
    chunks so it never has to fit in one message.

    Fields:
        term                — leader's current term
        leader_id           — so followers know who leads
        last_included_index — the snapshot replaces entries 1..this index
        last_included_term  — term of that entry
        offset              — where this chunk starts in the snapshot file
        data                — the chunk, base64-encoded
        done                — True on the last chunk
        config              — cluster membership as of last_included_index
        leader_address      — leader's Raft "host:port"
    """
    term:                int
    leader_id:           str
    last_included_index: int
    last_included_term:  int
    offset:              int
    data:                str
    done:                bool
    config:              Optional[list] = None
    leader_address:      str = ""


@dataclass
class InstallSnapshotResponse:
    """success=False: chunk out of order — the leader restarts from offset 0."""
    term:    int
    success: bool = True


@dataclass
class ReadIndexRequest:
    term:    int
    node_id: str


@dataclass
class ReadIndexResponse:
    """success=True: once you have applied up to `index`, your reads are current."""
    term:    int
    success: bool
    index:   int = 0


@dataclass
class ForwardRequest:
    """A client write, forwarded from a follower to the leader."""
    command:    str
    username:   str
    request_id: str
    timeout:    float


@dataclass
class ForwardResponse:
    ok:     bool
    result: Any = None
    error:  str = ""     # "not_leader" means "ask the new leader"


@dataclass
class TimeoutNowRequest:
    term:      int
    leader_id: str


@dataclass
class TimeoutNowResponse:
    term: int


# ── Serialization helpers ─────────────────────────────────────────────

def encode(obj) -> bytes:
    """Serialize a dataclass to JSON bytes with a newline terminator."""
    return (json.dumps(asdict(obj)) + "\n").encode("utf-8")


def decode_request_vote_req(data: str) -> RequestVoteRequest:
    d = json.loads(data)
    return RequestVoteRequest(**d)


def decode_request_vote_resp(data: str) -> RequestVoteResponse:
    d = json.loads(data)
    return RequestVoteResponse(**d)


def decode_append_entries_req(data: str) -> AppendEntriesRequest:
    d = json.loads(data)
    d["entries"] = [LogEntry(**e) for e in d["entries"]]
    return AppendEntriesRequest(**d)


def decode_append_entries_resp(data: str) -> AppendEntriesResponse:
    d = json.loads(data)
    return AppendEntriesResponse(**d)


def decode_install_snapshot_req(data: str) -> InstallSnapshotRequest:
    return InstallSnapshotRequest(**json.loads(data))


def decode_install_snapshot_resp(data: str) -> InstallSnapshotResponse:
    return InstallSnapshotResponse(**json.loads(data))


def decode_read_index_req(data: str) -> ReadIndexRequest:
    return ReadIndexRequest(**json.loads(data))


def decode_read_index_resp(data: str) -> ReadIndexResponse:
    return ReadIndexResponse(**json.loads(data))


def decode_forward_req(data: str) -> ForwardRequest:
    return ForwardRequest(**json.loads(data))


def decode_forward_resp(data: str) -> ForwardResponse:
    return ForwardResponse(**json.loads(data))


def decode_timeout_now_req(data: str) -> TimeoutNowRequest:
    return TimeoutNowRequest(**json.loads(data))


def decode_timeout_now_resp(data: str) -> TimeoutNowResponse:
    return TimeoutNowResponse(**json.loads(data))
