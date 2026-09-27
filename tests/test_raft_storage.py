"""
tests/test_raft_storage.py — Raft persistence

The same contract is checked against both storages:
  MemoryRaftStorage — RAM (single node, tests)
  DjangoRaftStorage — the raft_meta / raft_log tables (SQLite in tests)
"""
import pytest

from raft import MemoryRaftStorage, RaftEngine, LogEntry
from raft.rpc import AppendEntriesRequest, RequestVoteRequest


def entry(index: int, term: int = 1) -> LogEntry:
    return LogEntry(term=term, index=index, command=f"SET k{index} v", username="u")


@pytest.fixture(params=["memory", "django"])
def storage(request):
    if request.param == "memory":
        return MemoryRaftStorage()
    request.getfixturevalue("clean_db")
    from apps.cluster.raft_storage import DjangoRaftStorage
    return DjangoRaftStorage("node1")


class TestRaftStorageContract:

    def test_empty_storage_loads_defaults(self, storage):
        state = storage.load()
        assert (state.current_term, state.voted_for, state.log) == (0, None, [])
        assert (state.snapshot_index, state.last_applied) == (0, 0)

    def test_term_and_vote_round_trip(self, storage):
        storage.save_term_and_vote(7, "node2")
        state = storage.load()
        assert state.current_term == 7
        assert state.voted_for == "node2"

    def test_entries_load_in_order(self, storage):
        storage.append([entry(1), entry(2)])
        storage.append([entry(3, 2)])
        assert [(e.index, e.term) for e in storage.load().log] == [(1, 1), (2, 1), (3, 2)]

    def test_append_replaces_entries_from_its_first_index(self, storage):
        storage.append([entry(1), entry(2), entry(3)])
        storage.append([entry(2, 5)])
        assert [(e.index, e.term) for e in storage.load().log] == [(1, 1), (2, 5)]

    def test_truncate_from(self, storage):
        storage.append([entry(1), entry(2), entry(3)])
        storage.truncate_from(2)
        assert [e.index for e in storage.load().log] == [1]

    def test_snapshot_drops_covered_entries(self, storage):
        storage.append([entry(i) for i in range(1, 6)])
        storage.save_snapshot(index=3, term=1, last_applied=4)
        state = storage.load()
        assert [e.index for e in state.log] == [4, 5]
        assert (state.snapshot_index, state.snapshot_term, state.last_applied) == (3, 1, 4)

    def test_snapshot_keeps_term_and_vote(self, storage):
        storage.save_term_and_vote(3, "node1")
        storage.save_snapshot(index=0, term=0, last_applied=0)
        assert storage.load().current_term == 3


class TestEngineWithDatabaseStorage:

    def test_engine_restores_from_mysql_tables(self, clean_db):
        from apps.cluster.raft_storage import DjangoRaftStorage

        def make():
            return RaftEngine("node1", "127.0.0.1", 1, ["127.0.0.1:2"],
                              storage=DjangoRaftStorage("node1"))

        first = make()
        first._handle_request_vote(RequestVoteRequest(
            term=2, candidate_id="node2", last_log_index=0, last_log_term=0))
        first._handle_append_entries(AppendEntriesRequest(
            term=2, leader_id="node2", prev_log_index=0, prev_log_term=0,
            entries=[entry(1, 2), entry(2, 2)], leader_commit=0))

        restarted = make()
        assert restarted.get_term() == 2
        assert restarted._node.voted_for == "node2"
        assert [e.index for e in restarted._node.log] == [1, 2]

    def test_nodes_sharing_a_database_do_not_mix(self, clean_db):
        from apps.cluster.raft_storage import DjangoRaftStorage
        a, b = DjangoRaftStorage("node1"), DjangoRaftStorage("node2")
        a.save_term_and_vote(5, "node1")
        b.append([entry(1)])
        assert a.load().log == []
        assert b.load().current_term == 0
