"""
scripts/import_data.py — copy an existing database into a running cluster.

    python scripts/import_data.py --source-db distributed_cache       # MySQL
    python scripts/import_data.py --source-sqlite old_cache.sqlite3

Typical use: you ran a single node (python main.py) for a while and now
start a cluster — its databases begin empty. This reads every user and
key from the old database (read-only; MySQL credentials come from
DB_USER / DB_PASSWORD / DB_HOST / DB_PORT like the server) and sends
them to the cluster through the admin session. They go through Raft like
any other write, so every node gets them.

Safe to re-run: existing users are kept as they are, keys are simply
written again. Needs the admin token (--admin-token, $CACHE_ADMIN_TOKEN
or "admin_token" in cluster.json).
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from client.cli import (CacheClient, ClientError, load_cluster_json,  # noqa: E402
                        nodes_from_cluster, resolve_ssl)

FETCH_SIZE = 1000


def read_users(conn):
    """Yield (username, password_hash) from the source database."""
    cur = conn.cursor()
    cur.execute("SELECT username, password_hash FROM cache_users ORDER BY id")
    yield from _rows(cur)


def read_entries(conn):
    """Yield (username, key, value, expire_at_ms or 0) from the source database."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM cache_entries WHERE 1 = 0")
    has_expiry = "expire_at" in [d[0] for d in cur.description]
    cur.execute(
        "SELECT u.username, e.cache_key, e.cache_value"
        + (", e.expire_at" if has_expiry else "") +
        " FROM cache_entries e JOIN cache_users u ON u.id = e.user_id ORDER BY e.id")
    for row in _rows(cur):
        username, key, value = row[:3]
        expire_at = row[3] if has_expiry else None
        yield username, key, value, expire_at or 0


def _rows(cursor):
    while True:
        batch = cursor.fetchmany(FETCH_SIZE)
        if not batch:
            return
        yield from batch


def import_into(client: CacheClient, users, entries, report=print) -> dict:
    """Send users, then entries, through an admin-authenticated client."""
    stats = {"users_created": 0, "users_existing": 0, "keys": 0, "keys_expired": 0}
    for username, password_hash in users:
        if client.import_user(username, password_hash):
            stats["users_created"] += 1
        else:
            stats["users_existing"] += 1
    for username, key, value, expire_at in entries:
        if client.import_set(username, key, value, expire_at) == "EXPIRED":
            stats["keys_expired"] += 1
        else:
            stats["keys"] += 1
            if stats["keys"] % 1000 == 0:
                report(f"  ...{stats['keys']} keys")
    return stats


def connect_source(args):
    if args.source_sqlite:
        import sqlite3
        if not Path(args.source_sqlite).exists():
            raise SystemExit(f"Error: {args.source_sqlite} not found")
        return sqlite3.connect(f"file:{args.source_sqlite}?mode=ro", uri=True)
    import MySQLdb
    return MySQLdb.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        user=os.environ.get("DB_USER", "root"),
        passwd=os.environ.get("DB_PASSWORD", "12345"),
        port=int(os.environ.get("DB_PORT", "3306")),
        db=args.source_db, charset="utf8mb4",
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Import an existing database into the cluster")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-db", help="MySQL database to read from")
    source.add_argument("--source-sqlite", help="SQLite file to read from")
    parser.add_argument("--cluster", default=str(ROOT / "cluster.json"))
    parser.add_argument("--nodes", help="comma-separated host:port list (default: from cluster.json)")
    parser.add_argument("--admin-token", help="default: $CACHE_ADMIN_TOKEN or cluster.json")
    parser.add_argument("--ca", help="CA certificate for TLS (default: from cluster.json)")
    args = parser.parse_args(argv)

    cluster = load_cluster_json(args.cluster)
    nodes = ([a.strip() for a in args.nodes.split(",")] if args.nodes
             else nodes_from_cluster(cluster) if cluster else ["127.0.0.1:8001"])
    token = args.admin_token or os.environ.get("CACHE_ADMIN_TOKEN") or (cluster or {}).get("admin_token")
    if not token:
        print("Error: an admin token is needed (--admin-token, CACHE_ADMIN_TOKEN or cluster.json)")
        return 1

    conn = connect_source(args)
    try:
        with CacheClient(nodes, ssl_context=resolve_ssl(args, cluster)) as client:
            client.admin_login(token)
            print(f"Importing into the cluster via {client.address} ...")
            stats = import_into(client, read_users(conn), read_entries(conn))
    except ClientError as e:
        print(f"Error: {e}")
        return 1
    finally:
        conn.close()
    print(f"Done: {stats['users_created']} users created, {stats['users_existing']} already existed, "
          f"{stats['keys']} keys imported, {stats['keys_expired']} already expired (skipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
