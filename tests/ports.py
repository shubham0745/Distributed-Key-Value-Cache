"""
tests/ports.py — free ports for the test servers.

Binding to port 0 hands out a port from the OS's ephemeral range, the same
range outgoing connections draw from. Between picking such a port and the
server binding it, a node dialing a peer that isn't up yet can be given
that very port for its own end, and the server then fails with "Address
already in use". So pick ports below every OS's ephemeral range instead
(Linux starts at 32768, Windows and macOS at 49152), and never hand the
same one out twice in a run.
"""
import random
import socket

PORT_RANGE = (20000, 32000)
_handed_out: set[int] = set()


def get_free_port() -> int:
    for _ in range(1000):
        port = random.randint(*PORT_RANGE)
        if port in _handed_out:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:          # taken, or reserved by the OS
                continue
        _handed_out.add(port)
        return port
    raise RuntimeError(f"no free port in {PORT_RANGE}")
