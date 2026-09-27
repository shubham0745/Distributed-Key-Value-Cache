"""
Entry point for the Distributed Key-Value Cache Server.

Single node (Weeks 1-3 behaviour):
    python main.py                      # MySQL-backed, port 8001
    python main.py --no-db              # RAM only, no MySQL needed

Raft cluster from cluster.json (Weeks 4-6), one terminal per node:
    python main.py --node node1
    python main.py --node node2
    python main.py --node node3
or all at once:
    python scripts/run_cluster.py [--no-db]

First run with MySQL — create + migrate the node's database:
    python main.py --init-db                # single node
    python main.py --node node1 --init-db   # one cluster node

Then in another terminal, connect with:
    python client/cli.py                # follows leader redirects for you
    telnet 127.0.0.1 8001
"""
import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Distributed key-value cache server")
    parser.add_argument("--node", help="node id from the cluster config (omit for a single node)")
    parser.add_argument("--cluster", default=str(ROOT / "cluster.json"),
                        help="cluster config file (default: cluster.json)")
    parser.add_argument("--host", default="127.0.0.1", help="single-node bind address")
    parser.add_argument("--port", type=int, default=int(os.environ.get("CACHE_PORT", 8001)),
                        help="single-node client port (default: $CACHE_PORT or 8001)")
    parser.add_argument("--no-db", action="store_true",
                        help="keep everything in RAM; nothing survives a restart")
    parser.add_argument("--init-db", action="store_true",
                        help="create this node's database if needed, run migrations, exit")
    parser.add_argument("--stale-reads", action="store_true",
                        help="let followers answer GET/HAS locally (faster, may lag the leader)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    cluster = None
    if args.node:
        from config.cluster import load_cluster_config
        try:
            cluster = load_cluster_config(args.cluster)
            node = cluster.get(args.node)
        except ValueError as e:
            sys.exit(f"Error: {e}")
        if node.db_name:
            os.environ["DB_NAME"] = node.db_name    # each node: its own database

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
                                               stale_reads=args.stale_reads)
        where = f"node '{args.node}' on {server.host}:{server.port} (raft port {server.raft.port})"
    else:
        server = TCPServer(host=args.host, port=args.port, use_db=use_db)
        where = f"{server.host}:{server.port}"

    print(f"Starting Distributed Cache Server - {where}")
    print(f"Storage: {'MySQL (' + os.environ.get('DB_NAME', 'distributed_cache') + ')' if use_db else 'RAM only'}")
    print("Press Ctrl+C to stop\n")

    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()
    except StateLoadError as e:
        sys.exit(f"Error: {e}\n"
                 f"Is MySQL running and migrated? Try: python main.py "
                 f"{'--node ' + args.node + ' ' if args.node else ''}--init-db\n"
                 f"Or run without a database: add --no-db")
    except OSError as e:
        server.stop()
        sys.exit(f"Error: could not open a port ({e}). Is another server already running?")


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
