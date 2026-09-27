"""
scripts/run_cluster.py — start every node in cluster.json from one terminal.

    python scripts/run_cluster.py              # MySQL-backed nodes
    python scripts/run_cluster.py --no-db      # RAM only, no MySQL needed
    python scripts/run_cluster.py --init-db    # create + migrate each node's DB, then exit

Output lines are prefixed with the node id. Ctrl+C stops every node.
To watch a failover, run each node in its own terminal instead
(python main.py --node node1, ...) and stop the leader.
"""
import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.cluster import load_cluster_config  # noqa: E402


def pump(node_id: str, stream):
    for line in iter(stream.readline, ""):
        print(f"[{node_id}] {line}", end="", flush=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run every node of the cluster")
    parser.add_argument("--cluster", default=str(ROOT / "cluster.json"))
    parser.add_argument("--no-db", action="store_true", help="RAM-only nodes")
    parser.add_argument("--stale-reads", action="store_true",
                        help="let followers answer GET/HAS locally")
    parser.add_argument("--init-db", action="store_true",
                        help="create + migrate every node's database, then exit")
    args = parser.parse_args(argv)

    try:
        cluster = load_cluster_config(args.cluster)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    base = [sys.executable, str(ROOT / "main.py"), "--cluster", args.cluster]

    if args.init_db:
        for node in cluster.nodes:
            print(f"== {node.id} ==")
            subprocess.run(base + ["--node", node.id, "--init-db"], cwd=ROOT, check=True)
        return 0

    env = dict(os.environ, PYTHONUNBUFFERED="1")
    procs = []
    for node in cluster.nodes:
        cmd = base + ["--node", node.id]
        cmd += ["--no-db"] if args.no_db else []
        cmd += ["--stale-reads"] if args.stale_reads else []
        proc = subprocess.Popen(cmd, cwd=ROOT, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        threading.Thread(target=pump, args=(node.id, proc.stdout), daemon=True).start()
        procs.append((node.id, proc))

    print(f"Started {len(procs)} nodes - Ctrl+C to stop them all")
    try:
        running = set(node_id for node_id, _ in procs)
        while running:
            for node_id, proc in procs:
                if node_id in running and proc.poll() is not None:
                    print(f"[{node_id}] exited with code {proc.returncode}")
                    running.discard(node_id)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping cluster...")
    finally:
        for _, proc in procs:
            if proc.poll() is None:
                proc.terminate()
        for _, proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    return 0


if __name__ == "__main__":
    sys.exit(main())
