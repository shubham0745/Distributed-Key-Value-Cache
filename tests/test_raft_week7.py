"""
tests/test_raft_week7.py — the Raft extensions

  - Pre-Vote + leader stickiness: an isolated node can't inflate its term
    and disrupt the cluster when it comes back
  - Check-quorum: a leader cut off from the majority steps down
  - ReadIndex: linearizable reads on the leader and on followers
  - Write forwarding: submit() works on any node
  - Membership changes: add / remove nodes, one at a time
  - Leadership transfer on a graceful shutdown
  - Snapshots streamed in chunks
"""
import time

import pytest

import raft.node as raft_node
from raft import (RaftEngine, LogEntry, Member, NotLeaderError, ProposalError,
                  ConfigChangeError, config_command)
from raft.rpc import RequestVoteRequest, AppendEntriesRequest
from tests.test_raft import RaftCluster, KVMachine, wait_until, entry, make_engine


@pytest.fixture
def cluster():
    c = RaftCluster()
    yield c
    c.stop_all()


# ──────────────────────────────────────────────
# PRE-VOTE, LEADER STICKINESS, CHECK-QUORUM
# ──────────────────────────────────────────────

class TestPreVoteUnit:

    def test_pre_vote_changes_nothing(self):
        engine, _ = make_engine()
        resp = engine._handle_request_vote(RequestVoteRequest(
            term=5, candidate_id="other", last_log_index=0, last_log_term=0, pre_vote=True))
        assert resp.vote_granted is True
        assert engine.get_term() == 0            # no term adopted
        assert engine._node.voted_for is None    # no vote recorded

    def test_pre_vote_refused_while_leader_is_alive(self):
        engine, _ = make_engine()
        engine._handle_append_entries(AppendEntriesRequest(
            term=1, leader_id="leader", prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0))
        resp = engine._handle_request_vote(RequestVoteRequest(
            term=2, candidate_id="other", last_log_index=0, last_log_term=0, pre_vote=True))
        assert resp.vote_granted is False

    def test_disruptive_vote_ignored_while_leader_is_alive(self):
        engine, _ = make_engine()
        engine._handle_append_entries(AppendEntriesRequest(
            term=1, leader_id="leader", prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0))
        resp = engine._handle_request_vote(RequestVoteRequest(
            term=9, candidate_id="rejoined", last_log_index=0, last_log_term=0))
        assert resp.vote_granted is False
        assert engine.get_term() == 1            # the high term was NOT adopted

    def test_transfer_vote_bypasses_stickiness(self):
        engine, _ = make_engine()
        engine._handle_append_entries(AppendEntriesRequest(
            term=1, leader_id="leader", prev_log_index=0, prev_log_term=0,
            entries=[], leader_commit=0))
        resp = engine._handle_request_vote(RequestVoteRequest(
            term=2, candidate_id="successor", last_log_index=0, last_log_term=0, transfer=True))
        assert resp.vote_granted is True


class TestPartitions:

    def test_isolated_node_does_not_inflate_its_term(self, cluster):
        cluster.start()
        leader = cluster.leader()
        loner = next(i for i, e in enumerate(cluster.engines) if e is not leader)
        term = leader.get_term()

        cluster.isolate(loner)
        time.sleep(3)                            # many election timeouts
        assert cluster.engines[loner].get_term() == term   # pre-votes failed, no bumps

        cluster.heal()
        time.sleep(1)
        assert leader.is_leader()                # not disrupted on return
        assert leader.get_term() == term

    def test_partitioned_leader_steps_down(self, cluster):
        cluster.start()
        old = cluster.leader()
        old_index = cluster.engines.index(old)
        cluster.isolate(old_index)

        assert wait_until(lambda: not old.is_leader(), timeout=3)     # check-quorum
        others = [i for i in range(3) if i != old_index]
        new = cluster.leader(among=others)
        assert new.get_term() > old.get_term() or new.get_term() == old.get_term() + 1
        with pytest.raises((NotLeaderError, ProposalError)):
            old.propose("SET x 1", "u", timeout=0.3)

        new.propose("SET during partition", "u")
        cluster.heal()
        assert wait_until(lambda: cluster.machines[old_index].data.get("during") == "partition")
        assert old.get_leader() == new.node_id


# ──────────────────────────────────────────────
# READINDEX + FORWARDING
# ──────────────────────────────────────────────

class TestReadIndex:

    def test_single_node(self):
        machine = KVMachine()
        engine = RaftEngine("solo", "127.0.0.1", None, [], apply_fn=machine.apply)
        engine.start()
        try:
            engine.propose("SET a 1", "u")
            assert engine.read_index(timeout=1) >= 2
        finally:
            engine.stop()

    def test_follower_read_sees_the_latest_write(self, cluster):
        cluster.start()
        leader = cluster.leader()
        followers = [e for e in cluster.engines if e is not leader]
        for i in range(10):
            leader.propose(f"SET k {i}", "u")
            for follower in followers:
                follower.read_index(timeout=2)
                assert cluster.machine_of(follower).data["k"] == str(i)

    def test_partitioned_leader_cannot_serve_reads(self, cluster):
        cluster.start()
        leader = cluster.leader()
        cluster.isolate(cluster.engines.index(leader))
        with pytest.raises((NotLeaderError, ProposalError)):
            leader.read_index(timeout=0.5)


class TestForwarding:

    def test_submit_on_a_follower(self, cluster):
        cluster.start()
        leader = cluster.leader()
        follower = next(e for e in cluster.engines if e is not leader)
        assert wait_until(lambda: follower.get_leader() == leader.node_id)
        assert follower.submit("SET via follower", "u") is True
        assert follower.submit("DELETE via", "u") is True
        assert wait_until(lambda: all("via" not in m.data and m.applied for m in cluster.machines))

    def test_submit_waits_out_an_election(self, cluster):
        cluster.start()
        old = cluster.leader()
        follower = next(e for e in cluster.engines if e is not old)
        old.stop()
        # No leader right now — submit() waits for the next one
        assert follower.submit("SET after election", "u", timeout=5) is True


# ──────────────────────────────────────────────
# MEMBERSHIP CHANGES
# ──────────────────────────────────────────────

class TestMembership:

    def test_add_a_node(self, cluster):
        cluster.start()
        leader = cluster.leader()
        for i in range(30):
            leader.propose(f"SET k{i} v{i}", "u")

        new = cluster.add_joining()
        time.sleep(0.5)
        assert cluster.engines[new].members() == []          # not a member yet
        leader.add_member(cluster.member(new))

        for e in cluster.engines:
            assert wait_until(lambda: len(e.members()) == 4)
        assert wait_until(lambda: cluster.machines[new].data == cluster.machine_of(leader).data)
        leader.propose("SET after add", "u")
        assert wait_until(lambda: cluster.machines[new].data.get("after") == "add")

    def test_new_node_counts_towards_the_majority(self, cluster):
        cluster.start()
        leader = cluster.leader()
        new = cluster.add_joining()
        leader.add_member(cluster.member(new))
        # 4 members → majority 3. Stop one old follower: 3 left, still fine.
        follower = next(e for e in cluster.engines[:3] if e is not leader)
        follower.stop()
        assert leader.propose("SET still works", "u", timeout=3) is True

    def test_add_catches_up_through_a_snapshot(self):
        cluster = RaftCluster(snapshot_threshold=5)
        try:
            cluster.start()
            leader = cluster.leader()
            for i in range(40):
                leader.propose(f"SET k{i} v{i}", "u")
            assert leader._node.snapshot_index > 0
            new = cluster.add_joining()
            leader.add_member(cluster.member(new))
            assert wait_until(lambda: cluster.machines[new].data == cluster.machine_of(leader).data)
        finally:
            cluster.stop_all()

    def test_remove_a_follower(self, cluster):
        cluster.start()
        leader = cluster.leader()
        victim = next(e for e in cluster.engines if e is not leader)
        leader.remove_member(victim.node_id)
        assert [m.node_id for m in leader.members()] == sorted(
            e.node_id for e in cluster.engines if e is not victim)
        victim.stop()
        assert leader.propose("SET two nodes", "u") is True   # 2 of 2

    def test_remove_the_leader(self, cluster):
        cluster.start()
        old = cluster.leader()
        old.remove_member(old.node_id)
        assert wait_until(lambda: not old.is_leader())
        others = [i for i, e in enumerate(cluster.engines) if e is not old]
        new = cluster.leader(among=others)
        assert old.node_id not in [m.node_id for m in new.members()]
        time.sleep(1.5)
        assert not old.is_leader()               # not a member: never campaigns again

    def test_membership_survives_a_restart(self, cluster):
        cluster.start()
        leader = cluster.leader()
        victim = next(i for i, e in enumerate(cluster.engines) if e is not leader)
        leader.remove_member(cluster.engines[victim].node_id)
        survivor = next(i for i, e in enumerate(cluster.engines)
                        if e is not leader and i != victim)
        cluster.restart(survivor)
        assert len(cluster.engines[survivor].members()) == 2

    def test_invalid_changes_are_refused(self, cluster):
        cluster.start()
        leader = cluster.leader()
        with pytest.raises(ConfigChangeError):
            leader.remove_member("ghost")
        with pytest.raises(ConfigChangeError):
            leader.add_member(Member(leader.node_id, "127.0.0.1:1"))
        follower = next(e for e in cluster.engines if e is not leader)
        with pytest.raises(NotLeaderError):
            follower.add_member(Member("node9", "127.0.0.1:1"))

    def test_uncommitted_config_is_undone_by_a_conflict(self):
        engine, _ = make_engine()
        members = [Member("a", "h:1"), Member("b", "h:2")]
        engine._handle_append_entries(AppendEntriesRequest(
            term=1, leader_id="a", prev_log_index=0, prev_log_term=0,
            entries=[entry(1), LogEntry(1, 2, config_command(members), "")], leader_commit=1))
        assert sorted(m.node_id for m in engine.members()) == ["a", "b"]
        # A new leader never had entry 2: it overwrites it — membership reverts
        engine._handle_append_entries(AppendEntriesRequest(
            term=2, leader_id="c", prev_log_index=1, prev_log_term=1,
            entries=[entry(2, 2)], leader_commit=1))
        assert [m.node_id for m in engine.members()] == [engine.node_id]


# ──────────────────────────────────────────────
# LEADERSHIP TRANSFER
# ──────────────────────────────────────────────

class TestLeadershipTransfer:

    def test_transfer_to_a_caught_up_follower(self, cluster):
        cluster.start()
        leader = cluster.leader()
        leader.propose("SET before handover", "u")
        term = leader.get_term()
        started = time.time()
        assert leader.transfer_leadership() is True
        new = cluster.leader()
        assert new is not leader
        assert new.get_term() == term + 1
        assert time.time() - started < 2.0
        new.propose("SET after handover", "u")
        assert wait_until(lambda: cluster.machine_of(leader).data.get("after") == "handover")

    def test_transfer_with_no_followers_is_refused(self):
        engine = RaftEngine("solo", "127.0.0.1", None, [])
        engine.start()
        try:
            assert engine.transfer_leadership() is False
            assert engine.is_leader()
        finally:
            engine.stop()


# ──────────────────────────────────────────────
# CHUNKED SNAPSHOTS
# ──────────────────────────────────────────────

class TestChunkedSnapshots:

    def test_snapshot_sent_in_many_chunks(self, monkeypatch):
        monkeypatch.setattr(raft_node, "SNAPSHOT_CHUNK_SIZE", 64)
        cluster = RaftCluster(snapshot_threshold=5)
        try:
            cluster.start(0, 1)
            leader = cluster.leader(among=[0, 1])
            for i in range(30):
                leader.propose(f"SET key{i} {'x' * 20}", "u")
            cluster.start(2)
            expected = cluster.machine_of(leader).data
            assert wait_until(lambda: cluster.machines[2].data == expected)
        finally:
            cluster.stop_all()
