"""
config/cluster.py

Reads cluster.json — the list of nodes in the Raft cluster.

    {
      "nodes": [
        {"id": "node1", "host": "127.0.0.1", "client_port": 8001,
         "raft_port": 9001, "db_name": "distributed_cache_node1"},
        ...
      ]
    }

client_port — where users connect (telnet / client/cli.py)
raft_port   — where nodes talk Raft to each other
db_name     — this node's own MySQL database (optional)
"""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class NodeConfig:
    id: str
    host: str
    client_port: int
    raft_port: int
    db_name: Optional[str] = None

    @property
    def client_address(self) -> str:
        return f"{self.host}:{self.client_port}"

    @property
    def raft_address(self) -> str:
        return f"{self.host}:{self.raft_port}"


@dataclass(frozen=True)
class ClusterConfig:
    nodes: tuple[NodeConfig, ...]

    def get(self, node_id: str) -> NodeConfig:
        for node in self.nodes:
            if node.id == node_id:
                return node
        known = ", ".join(n.id for n in self.nodes)
        raise ValueError(f"Unknown node '{node_id}'. Nodes in the cluster: {known}")

    def peers_of(self, node_id: str) -> list[str]:
        """Raft addresses of every OTHER node."""
        return [n.raft_address for n in self.nodes if n.id != node_id]

    def client_addresses(self) -> dict[str, str]:
        """node_id → client address, used to redirect clients to the leader."""
        return {n.id: n.client_address for n in self.nodes}


def load_cluster_config(path) -> ClusterConfig:
    path = Path(path)
    if not path.exists():
        raise ValueError(f"Cluster config not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))

    nodes = []
    for raw in data.get("nodes", []):
        try:
            nodes.append(NodeConfig(
                id=str(raw["id"]),
                host=raw.get("host", "127.0.0.1"),
                client_port=int(raw["client_port"]),
                raft_port=int(raw["raft_port"]),
                db_name=raw.get("db_name"),
            ))
        except KeyError as e:
            raise ValueError(f"{path}: node entry {raw} is missing {e}") from None

    if not nodes:
        raise ValueError(f"{path}: no nodes defined")
    ids = [n.id for n in nodes]
    if len(set(ids)) != len(ids):
        raise ValueError(f"{path}: node ids must be unique, got {ids}")
    addresses = [a for n in nodes for a in (n.client_address, n.raft_address)]
    if len(set(addresses)) != len(addresses):
        raise ValueError(f"{path}: every client_port / raft_port must be different")
    return ClusterConfig(nodes=tuple(nodes))
