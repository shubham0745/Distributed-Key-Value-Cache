"""
config/cluster.py

Reads cluster.json — the nodes of the Raft cluster.

    {
      "nodes": [
        {"id": "node1", "host": "127.0.0.1", "client_port": 8001,
         "raft_port": 9001, "db_name": "distributed_cache_node1"},
        ...
      ],
      "admin_token": "...",                                  (optional)
      "tls": {"ca": "certs/ca.pem", "cert_dir": "certs"}     (optional)
    }

client_port — where users connect (telnet / client/cli.py)
raft_port   — where nodes talk Raft to each other
db_name     — this node's own MySQL database (optional)
join        — true: not part of the starting membership; when started,
              the node asks the running cluster to add it (optional)
admin_token — enables admin commands (ADDNODE, REMOVENODE, IMPORT...).
              The CACHE_ADMIN_TOKEN environment variable overrides it.
tls         — encrypt everything; certificates from scripts/gen_certs.py
              (<cert_dir>/<node id>.pem + <node id>-key.pem). Paths are
              relative to cluster.json.

The node list is the membership a brand-new cluster starts with. Once a
node has run, its membership lives in its Raft log: add or remove nodes
with ADDNODE / REMOVENODE (or `python main.py --node X --join`), not by
editing this file.
"""
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from raft.types import Member


@dataclass(frozen=True)
class NodeConfig:
    id: str
    host: str
    client_port: int
    raft_port: int
    db_name: Optional[str] = None
    join: bool = False

    @property
    def client_address(self) -> str:
        return f"{self.host}:{self.client_port}"

    @property
    def raft_address(self) -> str:
        return f"{self.host}:{self.raft_port}"

    def member(self) -> Member:
        return Member(self.id, self.raft_address, self.client_address)


@dataclass(frozen=True)
class TLSConfig:
    ca: str
    cert_dir: str

    def cert(self, node_id: str) -> str:
        return os.path.join(self.cert_dir, f"{node_id}.pem")

    def key(self, node_id: str) -> str:
        return os.path.join(self.cert_dir, f"{node_id}-key.pem")


@dataclass(frozen=True)
class ClusterConfig:
    nodes: tuple[NodeConfig, ...]
    tls: Optional[TLSConfig] = None
    config_admin_token: Optional[str] = None

    def get(self, node_id: str) -> NodeConfig:
        for node in self.nodes:
            if node.id == node_id:
                return node
        known = ", ".join(n.id for n in self.nodes)
        raise ValueError(f"Unknown node '{node_id}'. Nodes in the cluster: {known}")

    def peers_of(self, node_id: str) -> list[str]:
        """Raft addresses of every OTHER node."""
        return [n.raft_address for n in self.nodes if n.id != node_id]

    def members(self) -> list[Member]:
        """The membership a brand-new cluster starts with (join nodes come later)."""
        return [n.member() for n in self.nodes if not n.join]

    def client_addresses(self) -> dict[str, str]:
        """node_id → client address, used to redirect clients to the leader."""
        return {n.id: n.client_address for n in self.nodes}

    @property
    def admin_token(self) -> Optional[str]:
        return os.environ.get("CACHE_ADMIN_TOKEN") or self.config_admin_token

    # ── TLS contexts (None when TLS is off) ──

    def client_port_context(self, node_id: str):
        """What a node presents to clients."""
        if not self.tls:
            return None
        from config.tls import server_context
        return server_context(self.tls.cert(node_id), self.tls.key(node_id))

    def raft_server_context(self, node_id: str):
        """Node-to-node, receiving side: require a certificate from our CA."""
        if not self.tls:
            return None
        from config.tls import server_context
        return server_context(self.tls.cert(node_id), self.tls.key(node_id),
                              ca=self.tls.ca, require_client_cert=True)

    def raft_client_context(self, node_id: str):
        """Node-to-node, sending side: present our certificate, check theirs."""
        if not self.tls:
            return None
        from config.tls import client_context
        return client_context(self.tls.ca, self.tls.cert(node_id), self.tls.key(node_id))

    def client_context(self):
        """For client/cli.py: trust node certificates signed by our CA."""
        if not self.tls:
            return None
        from config.tls import client_context
        return client_context(self.tls.ca)


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
                join=bool(raw.get("join", False)),
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
    if all(n.join for n in nodes):
        raise ValueError(f"{path}: at least one node must not be a 'join' node")

    tls = None
    if data.get("tls"):
        base = path.resolve().parent
        tls = TLSConfig(ca=str(base / data["tls"]["ca"]),
                        cert_dir=str(base / data["tls"].get("cert_dir", "certs")))
    return ClusterConfig(nodes=tuple(nodes), tls=tls,
                         config_admin_token=data.get("admin_token"))
