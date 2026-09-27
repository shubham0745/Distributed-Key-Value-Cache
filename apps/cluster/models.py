"""
apps/cluster/models.py  (Week 6)

Durable Raft state for one node — see raft/storage.py for WHY it must
survive a crash.

  RaftMeta     — one row per node: term, vote, snapshot boundary
  RaftLogEntry — the node's log entries after the snapshot

Rows carry node_id so several nodes could share one database, but
normally each node has its own (cluster.json → db_name).
"""
from django.db import models


class RaftMeta(models.Model):
    node_id         = models.CharField(max_length=64, unique=True)
    current_term    = models.BigIntegerField(default=0)
    voted_for       = models.CharField(max_length=64, null=True, blank=True)
    snapshot_index  = models.BigIntegerField(default=0)
    snapshot_term   = models.BigIntegerField(default=0)
    last_applied    = models.BigIntegerField(default=0)
    snapshot_config = models.TextField(null=True, blank=True)   # JSON members at snapshot_index

    class Meta:
        db_table = "raft_meta"

    def __str__(self):
        return f"{self.node_id}: term={self.current_term} voted_for={self.voted_for}"


class RaftLogEntry(models.Model):
    node_id   = models.CharField(max_length=64)
    log_index = models.BigIntegerField()          # "index" is a reserved word in MySQL
    term      = models.BigIntegerField()
    command   = models.TextField()
    username  = models.CharField(max_length=150, blank=True)
    request_id = models.CharField(max_length=100, blank=True, default="")

    class Meta:
        db_table = "raft_log"
        unique_together = ("node_id", "log_index")

    def __str__(self):
        return f"{self.node_id}#{self.log_index} (term {self.term})"
