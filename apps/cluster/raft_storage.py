"""
apps/cluster/raft_storage.py  (Week 6)

RaftStorage backed by the RaftMeta / RaftLogEntry tables.

The engine calls these from many short-lived threads (one per RPC),
so every call first refreshes stale connections, and each thread
closes its own connection when it finishes (close_thread_resources).
"""
import json
from typing import Optional

from django.db import close_old_connections, connection, transaction

from raft.storage import RaftStorage, PersistentState
from raft.types import LogEntry, Member
from apps.cluster.models import RaftMeta, RaftLogEntry


class DjangoRaftStorage(RaftStorage):

    def __init__(self, node_id: str):
        self.node_id = node_id

    def load(self) -> PersistentState:
        close_old_connections()
        meta = RaftMeta.objects.filter(node_id=self.node_id).first() or RaftMeta(node_id=self.node_id)
        rows = (RaftLogEntry.objects
                .filter(node_id=self.node_id, log_index__gt=meta.snapshot_index)
                .order_by("log_index"))
        log = [LogEntry(term=r.term, index=r.log_index, command=r.command,
                        username=r.username, request_id=r.request_id)
               for r in rows]
        config = None
        if meta.snapshot_config is not None:
            config = [Member.from_dict(d) for d in json.loads(meta.snapshot_config)]
        return PersistentState(
            current_term=meta.current_term,
            voted_for=meta.voted_for,
            log=log,
            snapshot_index=meta.snapshot_index,
            snapshot_term=meta.snapshot_term,
            last_applied=meta.last_applied,
            snapshot_config=config,
        )

    def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        close_old_connections()
        RaftMeta.objects.update_or_create(
            node_id=self.node_id,
            defaults={"current_term": term, "voted_for": voted_for},
        )

    def append(self, entries: list[LogEntry]) -> None:
        if not entries:
            return
        close_old_connections()
        with transaction.atomic():
            RaftLogEntry.objects.filter(node_id=self.node_id,
                                        log_index__gte=entries[0].index).delete()
            RaftLogEntry.objects.bulk_create([
                RaftLogEntry(node_id=self.node_id, log_index=e.index, term=e.term,
                             command=e.command, username=e.username,
                             request_id=e.request_id)
                for e in entries
            ])

    def truncate_from(self, index: int) -> None:
        close_old_connections()
        RaftLogEntry.objects.filter(node_id=self.node_id, log_index__gte=index).delete()

    def save_snapshot(self, index: int, term: int, last_applied: int,
                      config: Optional[list] = None) -> None:
        close_old_connections()
        values = {"snapshot_index": index, "snapshot_term": term, "last_applied": last_applied}
        if config is not None:
            values["snapshot_config"] = json.dumps([m.to_dict() for m in config])
        with transaction.atomic():
            RaftMeta.objects.update_or_create(node_id=self.node_id, defaults=values)
            RaftLogEntry.objects.filter(node_id=self.node_id, log_index__lte=index).delete()

    def close_thread_resources(self) -> None:
        connection.close()
