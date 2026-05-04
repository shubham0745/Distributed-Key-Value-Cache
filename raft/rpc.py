"""
raft/rpc.py

RPC (Remote Procedure Call) message definitions for Raft.

Two RPCs exist in Raft:
  1. RequestVote    — sent by CANDIDATE to gather votes
  2. AppendEntries  — sent by LEADER for heartbeats AND log replication

We serialize these as JSON strings over TCP sockets between nodes.
"""
import json
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class RequestVoteRequest:
    """
    Sent by a CANDIDATE to every other node asking for their vote.

    Fields:
        term           — candidate's current term
        candidate_id   — who is asking for the vote
        last_log_index — index of candidate's last log entry
        last_log_term  — term of candidate's last log entry

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
    """
    term:           int
    leader_id:      str
    prev_log_index: int
    prev_log_term:  int
    entries:        list   # list of serialized LogEntry dicts
    leader_commit:  int


@dataclass
class AppendEntriesResponse:
    """
    Response to an AppendEntries RPC.

    Fields:
        term    — follower's current term (leader steps down if higher)
        success — True if follower accepted the entries
    """
    term:    int
    success: bool


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
    return AppendEntriesRequest(**d)


def decode_append_entries_resp(data: str) -> AppendEntriesResponse:
    d = json.loads(data)
    return AppendEntriesResponse(**d)