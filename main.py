"""
Entry point for the Distributed Key-Value Cache Server.

Single node:
    python main.py                      # MySQL-backed, port 8001
    python main.py --no-db              # RAM only, no MySQL needed

Raft cluster from cluster.json, one terminal per node:
    python main.py --node node1
    python main.py --node node2
    python main.py --node node3
or all at once:
    python scripts/run_cluster.py [--no-db]

Grow a running cluster: add node4 to cluster.json (with "join": true,
or start it with --join), then
    python main.py --node node4         # asks the cluster to add it

First run with MySQL — create + migrate the node's database:
    python main.py --init-db                # single node
    python main.py --node node1 --init-db   # one cluster node

Then in another terminal, connect with:
    python client/cli.py
    telnet 127.0.0.1 8001

Ctrl+C stops the node gracefully: a leader first hands leadership to
another node, so the cluster doesn't have to wait for an election.
"""
import argparse
import os
import re
import signal
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Distributed key-value cache server")
    parser.add_argument("--node", help="node id from the cluster config (omit for a single node)")
    parser.add_argument("--cluster", default=str(ROOT / "cluster.json"),
                        help="cluster config file (default: cluster.json)")
    parser.add_argument("--join", action="store_true",
                        help="start without membership and ask the running cluster to add this node")
    parser.add_argument("--host", default="127.0.0.1", help="single-node bind address")
    parser.add_argument("--port", type=int, default=int(os.environ.get("CACHE_PORT", 8001)),
                        help="single-node client port (default: $CACHE_PORT or 8001)")
    parser.add_argument("--no-db", action="store_true",
                        help="keep everything in RAM; nothing survives a restart")
    parser.add_argument("--init-db", action="store_true",
                        help="create this node's database if needed, run migrations, exit")
    parser.add_argument("--stale-reads", action="store_true",
                        help="let followers answer GET/HAS/TTL/KEYS locally (faster, may lag)")
    parser.add_argument("--tls-cert", help="single node: certificate to serve TLS with")
    parser.add_argument("--tls-key", help="single node: private key for --tls-cert")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    cluster = node = None
    if args.node:
        from config.cluster import load_cluster_config
        try:
            cluster = load_cluster_config(args.cluster)
            node = cluster.get(args.node)
        except ValueError as e:
            sys.exit(f"Error: {e}")
        if node.db_name:
            os.environ["DB_NAME"] = node.db_name    # each node: its own database
        args.join = args.join or node.join
    elif args.join:
        sys.exit("Error: --join needs --node")

    if args.no_db:
        # Django still needs a configured backend to start (we use it for
        # password hashing). SQLite needs no driver and is never opened.
        os.environ["DB_ENGINE"] = "sqlite"

    # ── Django setup ──────────────────────────────────────────
    # We need Django's ORM and password hashing utilities (make_password,
    # check_password). Django requires settings to be configured
    # before any of its modules are imported.
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

    import django
    django.setup()
    # ──────────────────────────────────────────────────────────

    if args.init_db:
        init_database()
        return

    from server.tcp_server import TCPServer
    from server.state_machine import StateLoadError

    use_db = not args.no_db
    if cluster:
        server = TCPServer.from_cluster_config(cluster, args.node, use_db=use_db,
                                               join=args.join, stale_reads=args.stale_reads)
        where = f"node '{args.node}' on {server.host}:{server.port} (raft port {server.raft.port})"
    else:
        ssl_context = None
        if args.tls_cert:
            from config.tls import server_context
            ssl_context = server_context(args.tls_cert, args.tls_key)
        server = TCPServer(host=args.host, port=args.port, use_db=use_db,
                           admin_token=os.environ.get("CACHE_ADMIN_TOKEN"),
                           ssl_context=ssl_context)
        where = f"{server.host}:{server.port}"

    print(f"Starting Distributed Cache Server - {where}")
    print(f"Storage: {'MySQL (' + os.environ.get('DB_NAME', 'distributed_cache') + ')' if use_db else 'RAM only'}"
          f"{'  |  TLS on' if server.ssl_context else ''}")
    print("Press Ctrl+C to stop\n")

    if args.join:
        threading.Thread(target=join_cluster, args=(server, cluster, node),
                         daemon=True, name="join").start()

    # A stop request from a process manager (SIGTERM, or Ctrl+Break on
    # Windows) gets the same graceful shutdown as Ctrl+C.
    for name in ("SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), _raise_keyboard_interrupt)

    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop(graceful=True)
    except StateLoadError as e:
        sys.exit(f"Error: {e}\n"
                 f"Is MySQL running and migrated? Try: python main.py "
                 f"{'--node ' + args.node + ' ' if args.node else ''}--init-db\n"
                 f"Or run without a database: add --no-db")
    except OSError as e:
        server.stop()
        sys.exit(f"Error: could not open a port ({e}). Is another server already running?")


def _raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt


def join_cluster(server, cluster, node):
    """--join: once our server is up, ask the cluster (admin ADDNODE) to add us."""
    from client.cli import CacheClient, ClientError

    token = cluster.admin_token
    if not token:
        print("Error: --join needs an admin token (CACHE_ADMIN_TOKEN or admin_token in cluster.json)")
        return
    others = [n.client_address for n in cluster.nodes if n.id != node.id]
    while not server._running:
        time.sleep(0.1)
    while server._running:
        if node.id in [m.node_id for m in server.raft.members()]:
            print(f"{node.id} is a member of the cluster")
            return
        try:
            with CacheClient(others, ssl_context=cluster.client_context(), retry_for=15) as admin:
                admin.admin_login(token)
                admin.add_node(node.id, node.client_address, node.raft_address)
            print(f"{node.id} joined the cluster")
            return
        except ClientError as e:
            print(f"Join attempt failed ({e}) - retrying in 3s")
            time.sleep(3)


def init_database():
    """Create the configured database (MySQL only) and apply migrations."""
    from django.conf import settings
    from django.core.management import call_command

    db = settings.DATABASES["default"]
    if db["ENGINE"].endswith("mysql"):
        name = db["NAME"]
        if not re.fullmatch(r"[A-Za-z0-9_]+", name):
            sys.exit(f"Error: unsafe database name {name!r} (use letters, digits, _)")
        import MySQLdb
        conn = MySQLdb.connect(host=db["HOST"], user=db["USER"], passwd=db["PASSWORD"],
                               port=int(db["PORT"]))
        try:
            conn.cursor().execute(
                f"CREATE DATABASE IF NOT EXISTS `{name}` CHARACTER SET utf8mb4")
        finally:
            conn.close()
        print(f"Database '{name}' is ready")

    call_command("migrate", interactive=False)


if __name__ == "__main__":
    main()
