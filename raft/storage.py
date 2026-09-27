"""
raft/storage.py  (Week 6 — Raft persistence)

WHY PERSIST RAFT STATE?
Raft's safety proof assumes a node never forgets three things:
  current_term — or it could accept an old leader again
  voted_for    — or it could vote TWICE in one term → two leaders
  log          — or a committed entry could silently disappear
So the engine writes them through a RaftStorage BEFORE it answers any
RPC that depends on them. The snapshot boundary is stored too, with the
cluster membership in effect at that point (Week 7).

Two implementations:
  MemoryRaftStorage          — RAM only. Used by a single node (its data is
                               already durable in MySQL) and by tests.
  apps.cluster.raft_storage  — DjangoRaftStorage, backed by MySQL tables.
                               Lives in the Django app so this package
                               stays free of Django imports.
"""
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from raft.types import LogEntry


@dataclass
class PersistentState:
    """Everything a node reloads on startup."""
    current_term:    int = 0
    voted_for:       Optional[str] = None
    log:             list = field(default_factory=list)   # entries AFTER the snapshot
    snapshot_index:  int = 0
    snapshot_term:   int = 0
    last_applied:    int = 0
    snapshot_config: Optional[list] = None   # list[Member]; None = never saved


class RaftStorage(ABC):

    @abstractmethod
    def load(self) -> PersistentState:
        """Return the saved state (a fresh PersistentState if none)."""

    @abstractmethod
    def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        """Persist current_term + voted_for."""

    @abstractmethod
    def append(self, entries: list[LogEntry]) -> None:
        """Persist new log entries. Replaces any stored entry at the same index or later."""

    @abstractmethod
    def truncate_from(self, index: int) -> None:
        """Delete the entry at `index` and every entry after it."""

    @abstractmethod
    def save_snapshot(self, index: int, term: int, last_applied: int,
                      config: Optional[list] = None) -> None:
        """
        Record a snapshot boundary and drop the entries it covers (<= index).
        `config` is the membership at `index`; None keeps the stored one.
        """

    def close_thread_resources(self) -> None:
        """Called when an engine thread finishes (e.g. to close its DB connection)."""


class MemoryRaftStorage(RaftStorage):
    """
    Keeps the "persistent" state in RAM.

    It survives restarting a RaftEngine inside the same process (tests use
    that to simulate a crash + restart) but NOT a process restart.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._term = 0
        self._voted_for: Optional[str] = None
        self._entries: dict[int, LogEntry] = {}
        self._snapshot_index = 0
        self._snapshot_term = 0
        self._last_applied = 0
        self._config: Optional[list] = None

    def load(self) -> PersistentState:
        with self._lock:
            log = [self._entries[i] for i in sorted(self._entries) if i > self._snapshot_index]
            return PersistentState(
                current_term=self._term,
                voted_for=self._voted_for,
                log=log,
                snapshot_index=self._snapshot_index,
                snapshot_term=self._snapshot_term,
                last_applied=self._last_applied,
                snapshot_config=None if self._config is None else list(self._config),
            )

    def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        with self._lock:
            self._term = term
            self._voted_for = voted_for

    def append(self, entries: list[LogEntry]) -> None:
        if not entries:
            return
        with self._lock:
            self._drop_from(entries[0].index)
            for entry in entries:
                self._entries[entry.index] = entry

    def truncate_from(self, index: int) -> None:
        with self._lock:
            self._drop_from(index)

    def save_snapshot(self, index: int, term: int, last_applied: int,
                      config: Optional[list] = None) -> None:
        with self._lock:
            self._snapshot_index = index
            self._snapshot_term = term
            self._last_applied = last_applied
            if config is not None:
                self._config = list(config)
            for i in [i for i in self._entries if i <= index]:
                del self._entries[i]

    def _drop_from(self, index: int) -> None:
        for i in [i for i in self._entries if i >= index]:
            del self._entries[i]
