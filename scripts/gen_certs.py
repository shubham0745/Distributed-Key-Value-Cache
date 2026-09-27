"""
scripts/gen_certs.py — create the TLS certificates for a cluster.

    python scripts/gen_certs.py              # certs/ for every node in cluster.json
    python scripts/gen_certs.py --enable     # ...and switch TLS on in cluster.json

Creates:
    certs/ca.pem, certs/ca-key.pem           the cluster's own certificate authority
    certs/<node>.pem, certs/<node>-key.pem   one certificate per node

Each node certificate is valid for the node's host, localhost and
127.0.0.1, and for both roles of mutual TLS (server and client), because
nodes connect to each other in both directions. Clients only need
certs/ca.pem to verify the nodes. Keep the *-key.pem files private
(certs/ is in .gitignore).
"""
import argparse
import datetime
import ipaddress
import json
import sys
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

ROOT = Path(__file__).resolve().parent.parent
VALID_DAYS = 825


def _write_key(key, path: Path):
    path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ))
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _write_cert(cert, path: Path):
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def _name(common_name: str) -> x509.Name:
    return x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Distributed Key-Value Cache"),
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])


def create_ca(out_dir: Path):
    """A fresh certificate authority. Returns (cert, key)."""
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(_name("Distributed KV Cache CA"))
            .issuer_name(_name("Distributed KV Cache CA"))
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=5))
            .not_valid_after(now + datetime.timedelta(days=VALID_DAYS))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.KeyUsage(
                digital_signature=True, key_cert_sign=True, crl_sign=True,
                content_commitment=False, key_encipherment=False, data_encipherment=False,
                key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
            .sign(key, hashes.SHA256()))
    _write_cert(cert, out_dir / "ca.pem")
    _write_key(key, out_dir / "ca-key.pem")
    return cert, key


def create_node_cert(out_dir: Path, node_id: str, hosts: list[str], ca_cert, ca_key):
    """Certificate for one node, signed by the CA."""
    key = ec.generate_private_key(ec.SECP256R1())
    names = []
    for host in dict.fromkeys(hosts + ["localhost", "127.0.0.1"]):
        try:
            names.append(x509.IPAddress(ipaddress.ip_address(host)))
        except ValueError:
            names.append(x509.DNSName(host))
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(_name(node_id))
            .issuer_name(ca_cert.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=5))
            .not_valid_after(now + datetime.timedelta(days=VALID_DAYS))
            .add_extension(x509.SubjectAlternativeName(names), critical=False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,
                                                  ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .sign(ca_key, hashes.SHA256()))
    _write_cert(cert, out_dir / f"{node_id}.pem")
    _write_key(key, out_dir / f"{node_id}-key.pem")


def generate(out_dir, nodes: list[tuple[str, str]]) -> Path:
    """nodes: [(node_id, host), ...]. Writes the CA and one cert per node."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ca_cert, ca_key = create_ca(out_dir)
    for node_id, host in nodes:
        create_node_cert(out_dir, node_id, [host], ca_cert, ca_key)
    return out_dir


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Create TLS certificates for the cluster")
    parser.add_argument("--cluster", default=str(ROOT / "cluster.json"))
    parser.add_argument("--out", help="output directory (default: certs/ next to cluster.json)")
    parser.add_argument("--node", action="append", default=[],
                        help="extra node as id=host (e.g. node4=127.0.0.1); repeatable")
    parser.add_argument("--enable", action="store_true",
                        help="add the tls section to cluster.json")
    args = parser.parse_args(argv)

    cluster_path = Path(args.cluster)
    data = json.loads(cluster_path.read_text(encoding="utf-8"))
    nodes = [(n["id"], n.get("host", "127.0.0.1")) for n in data["nodes"]]
    for extra in args.node:
        node_id, _, host = extra.partition("=")
        nodes.append((node_id, host or "127.0.0.1"))

    out_dir = Path(args.out) if args.out else cluster_path.resolve().parent / "certs"
    generate(out_dir, nodes)
    print(f"Wrote CA and {len(nodes)} node certificates to {out_dir}")

    rel = Path(out_dir).resolve().relative_to(cluster_path.resolve().parent) \
        if Path(out_dir).resolve().is_relative_to(cluster_path.resolve().parent) else Path(out_dir).resolve()
    tls = {"ca": (rel / "ca.pem").as_posix(), "cert_dir": rel.as_posix()}
    if args.enable:
        data["tls"] = tls
        cluster_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        print(f"TLS enabled in {cluster_path} - restart the nodes to use it")
    else:
        print(f'To turn TLS on, add this to {cluster_path.name} (or rerun with --enable):')
        print(f'  "tls": {json.dumps(tls)}')
    return 0


if __name__ == "__main__":
    sys.exit(main())
