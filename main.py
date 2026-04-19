"""
Entry point for the Distributed Key-Value Cache Server.

Run this to start the server:
    python main.py

Then in another terminal, connect with:
    telnet 127.0.0.1 8001
    
Or use our CLI client (Week 5):
    python client/cli.py
"""
import sys
import os

# ── Django setup ──────────────────────────────────────────
# We need Django's password hashing utilities (make_password,
# check_password). Django requires settings to be configured
# before any of its modules are imported.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django
django.setup()
# ──────────────────────────────────────────────────────────

from server.tcp_server import TCPServer


def main():
    host = "127.0.0.1"
    port = int(os.environ.get("CACHE_PORT", 8001))

    server = TCPServer(host=host, port=port)

    print(f"Starting Distributed Cache Server on {host}:{port}")
    print("Press Ctrl+C to stop\n")

    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()