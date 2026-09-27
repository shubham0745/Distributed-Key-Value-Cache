"""
tests/test_tls.py — encryption between clients and nodes, and mutual TLS
between nodes, with certificates made by scripts/gen_certs.py.
"""
import json
import threading

import pytest

from client.cli import CacheClient, ClientError
from config.cluster import load_cluster_config
from config.tls import server_context, client_context
from raft import RaftEngine, Member
from raft.rpc import RequestVoteRequest, decode_request_vote_resp
from scripts import gen_certs
from server.tcp_server import TCPServer
from tests.test_cluster import FAST, fast_hashing, get_free_port, wait_until  # noqa: F401

NODES = [("node1", "127.0.0.1"), ("node2", "127.0.0.1"), ("node3", "127.0.0.1")]


@pytest.fixture(scope="module")
def certs(tmp_path_factory):
    return gen_certs.generate(tmp_path_factory.mktemp("certs"), NODES)


@pytest.fixture(scope="module")
def other_ca(tmp_path_factory):
    """A second, unrelated CA — a stranger's certificates."""
    return gen_certs.generate(tmp_path_factory.mktemp("rogue"), [("node1", "127.0.0.1")])


def contexts(certs, node_id):
    cert, key, ca = (str(certs / f"{node_id}.pem"), str(certs / f"{node_id}-key.pem"),
                     str(certs / "ca.pem"))
    return (server_context(cert, key),
            (server_context(cert, key, ca=ca, require_client_cert=True),
             client_context(ca, cert, key)))


@pytest.fixture
def tls_cluster(certs, fast_hashing):
    specs = [(node_id, get_free_port(), get_free_port()) for node_id, _ in NODES]
    members = [Member(n, f"127.0.0.1:{rp}", f"127.0.0.1:{cp}") for n, cp, rp in specs]
    servers = []
    for node_id, cp, rp in specs:
        client_ctx, raft_ctx = contexts(certs, node_id)
        servers.append(TCPServer("127.0.0.1", cp, use_db=False, node_id=node_id,
                                 raft_port=rp, members=members, ssl_context=client_ctx,
                                 raft_ssl=raft_ctx, raft_options=FAST))
    for srv in servers:
        threading.Thread(target=srv.start, daemon=True).start()
    assert wait_until(lambda: sum(s.raft.is_leader() for s in servers) == 1)
    yield servers
    for srv in servers:
        srv.stop()


def addresses(servers):
    return [f"{s.host}:{s.port}" for s in servers]


class TestTLS:

    def test_everything_works_over_tls(self, tls_cluster, certs):
        with CacheClient(addresses(tls_cluster),
                         ssl_context=client_context(str(certs / "ca.pem"))) as client:
            client.signup("secure", "pass1234")
            client.set("secret", "value")
            assert client.get("secret") == "value"
        # replication happened, so the nodes talked mutual TLS to each other
        for srv in tls_cluster:
            assert wait_until(lambda: srv.state_machine.get("secure", "secret") == "value")

    def test_plaintext_client_gets_a_helpful_error(self, tls_cluster):
        with CacheClient(addresses(tls_cluster)[:1], retry_for=0.5) as client:
            with pytest.raises(ClientError, match="uses TLS"):
                client.signup("plain", "pass1234")

    def test_client_refuses_an_untrusted_server(self, tls_cluster, other_ca):
        with CacheClient(addresses(tls_cluster), retry_for=1,
                         ssl_context=client_context(str(other_ca / "ca.pem"))) as client:
            with pytest.raises(ClientError):
                client.signup("fooled", "pass1234")

    def test_stranger_cannot_speak_raft(self, tls_cluster, certs, other_ca):
        target = tls_cluster[0].raft.raft_address
        vote = RequestVoteRequest(term=0, candidate_id="x", last_log_index=0, last_log_term=0,
                                  pre_vote=True)

        def engine_with(cert_dir, node_id):
            ca = str(certs / "ca.pem")          # trusts the real CA either way
            return RaftEngine(node_id, "127.0.0.1", None, [], ssl_client_context=client_context(
                ca, str(cert_dir / f"{node_id}.pem"), str(cert_dir / f"{node_id}-key.pem")))

        member = engine_with(certs, "node2")
        stranger = engine_with(other_ca, "node1")      # right name, wrong CA
        assert member._call(target, "RequestVote", vote, decode_request_vote_resp) is not None
        assert stranger._call(target, "RequestVote", vote, decode_request_vote_resp) is None


class TestConfig:

    def test_cluster_json_tls_section(self, tmp_path):
        gen_certs.generate(tmp_path / "certs", NODES)
        path = tmp_path / "cluster.json"
        path.write_text(json.dumps({
            "nodes": [{"id": n, "client_port": 8000 + i, "raft_port": 9000 + i}
                      for i, (n, _) in enumerate(NODES, 1)],
            "tls": {"ca": "certs/ca.pem", "cert_dir": "certs"},
        }))
        cluster = load_cluster_config(path)
        assert cluster.client_port_context("node1") is not None
        assert cluster.raft_server_context("node1") is not None
        assert cluster.raft_client_context("node1") is not None
        assert cluster.client_context() is not None

    def test_gen_certs_enable_writes_config(self, tmp_path, capsys):
        path = tmp_path / "cluster.json"
        path.write_text(json.dumps({"nodes": [{"id": "node1", "client_port": 8001,
                                               "raft_port": 9001}]}))
        assert gen_certs.main(["--cluster", str(path), "--enable"]) == 0
        data = json.loads(path.read_text())
        assert data["tls"] == {"ca": "certs/ca.pem", "cert_dir": "certs"}
        assert (tmp_path / "certs" / "node1-key.pem").exists()

    def test_no_tls_section_means_plaintext(self, tmp_path):
        path = tmp_path / "cluster.json"
        path.write_text(json.dumps({"nodes": [{"id": "node1", "client_port": 8001,
                                               "raft_port": 9001}]}))
        cluster = load_cluster_config(path)
        assert cluster.client_port_context("node1") is None
        assert cluster.client_context() is None
