"""
config/tls.py — TLS contexts from certificate files.

Two kinds of connections are encrypted:
  client → node   the node proves who it is (server certificate);
                  the client proves who it is with its password
  node ↔ node     MUTUAL TLS: both sides present a certificate signed by
                  the cluster CA, so a stranger can't join the cluster or
                  impersonate a node

scripts/gen_certs.py creates the CA and one certificate per node.
"""
import ssl
from typing import Optional


def server_context(cert: str, key: str, ca: Optional[str] = None,
                   require_client_cert: bool = False) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(cert, key)
    if require_client_cert:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(ca)
    return ctx


def client_context(ca: str, cert: Optional[str] = None,
                   key: Optional[str] = None) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)      # verifies cert + hostname
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(ca)
    if cert:
        ctx.load_cert_chain(cert, key)
    return ctx
