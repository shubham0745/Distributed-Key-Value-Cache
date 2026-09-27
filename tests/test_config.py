"""tests/test_config.py — reading and validating cluster.json."""
import json

import pytest

from config.cluster import load_cluster_config


def write(tmp_path, data) -> str:
    path = tmp_path / "cluster.json"
    path.write_text(json.dumps(data))
    return str(path)


def node(node_id, n, **extra):
    return {"id": node_id, "client_port": 8000 + n, "raft_port": 9000 + n, **extra}


class TestClusterConfig:

    def test_members_and_addresses(self, tmp_path):
        cluster = load_cluster_config(write(tmp_path, {"nodes": [node("a", 1), node("b", 2)]}))
        assert [m.node_id for m in cluster.members()] == ["a", "b"]
        assert cluster.peers_of("a") == ["127.0.0.1:9002"]
        assert cluster.client_addresses() == {"a": "127.0.0.1:8001", "b": "127.0.0.1:8002"}

    def test_join_nodes_are_not_in_the_starting_membership(self, tmp_path):
        cluster = load_cluster_config(write(tmp_path, {"nodes": [
            node("a", 1), node("b", 2), node("c", 3), node("d", 4, join=True)]}))
        assert [m.node_id for m in cluster.members()] == ["a", "b", "c"]
        assert cluster.get("d").join is True

    def test_admin_token_from_file_or_environment(self, tmp_path, monkeypatch):
        path = write(tmp_path, {"nodes": [node("a", 1)], "admin_token": "from-file"})
        monkeypatch.delenv("CACHE_ADMIN_TOKEN", raising=False)
        assert load_cluster_config(path).admin_token == "from-file"
        monkeypatch.setenv("CACHE_ADMIN_TOKEN", "from-env")
        assert load_cluster_config(path).admin_token == "from-env"

    @pytest.mark.parametrize("nodes, message", [
        ([], "no nodes"),
        ([node("a", 1), node("a", 2)], "unique"),
        ([node("a", 1), node("b", 1)], "different"),
        ([node("a", 1, join=True)], "join"),
        ([{"id": "a", "client_port": 8001}], "missing"),
    ])
    def test_invalid_configs(self, tmp_path, nodes, message):
        with pytest.raises(ValueError, match=message):
            load_cluster_config(write(tmp_path, {"nodes": nodes}))

    def test_unknown_node(self, tmp_path):
        cluster = load_cluster_config(write(tmp_path, {"nodes": [node("a", 1)]}))
        with pytest.raises(ValueError, match="Unknown node"):
            cluster.get("zzz")
