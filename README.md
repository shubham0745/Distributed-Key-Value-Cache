# Distributed Key-Value Cache

A Redis-style key-value cache written from scratch in Python. Several nodes stay in sync with the **Raft** consensus algorithm, and every node keeps a durable copy in **MySQL**. Leader election, log replication, persistence and snapshots are all implemented here, with no consensus library.

**Author:** Shubham Kumar

## Features

- **Per-user caches.** Each user has an isolated, thread-safe LRU cache (1000 keys per user by default).
- **Accounts.** `SIGNUP`/`LOGIN` with PBKDF2-hashed passwords (Django's hashers).
- **Write-through persistence.** Every write goes to MySQL before it reaches RAM. A key that the LRU evicts is read back from MySQL.
- **Raft cluster:**
  - leader election with randomized timeouts
  - log replication with the consistency check and conflict back-off
  - commitment by majority
  - persisted term, vote and log
  - log compaction, with `InstallSnapshot` for nodes that fall far behind
- **Leader redirects.** Followers answer `ERROR: NOT_LEADER <host:port>`. The bundled CLI client follows the redirect, logs in again and fails over when a node dies.
- **176 tests.** They cover unit behaviour, real multi-node clusters over TCP, crash and restart, and snapshot install.

## Architecture

```
            client/cli.py  or  telnet
                    │  one command per line
                    ▼
   ┌──────────────── node (main.py) ────────────────┐
   │ server/tcp_server.py   auth + commands          │
   │        │ writes                   │ reads       │
   │        ▼                          ▼             │
   │ raft/node.py  ──commit──▶  server/state_machine.py
   │  (RaftEngine)   apply()     per-user LRUCache    │
   │        │                          │ write-through│
   │        │ raft_meta / raft_log     ▼              │
   │        └──────────────▶  MySQL (this node's DB)  │
   └────────┬────────────────────────────────────────┘
            │ RequestVote / AppendEntries / InstallSnapshot (JSON over TCP)
            ▼
       other nodes (same layout, their own database)
```

**Write path (`SET`, `DELETE`, `SIGNUP`)**
1. The leader appends the command to its Raft log.
2. It replicates the entry to the followers.
3. Once a majority stores the entry, it is **committed**.
4. Every node applies committed entries in log order: MySQL first, then RAM.
5. The client gets `OK` only after the leader has applied the entry.

**Read path (`GET`, `HAS`)**
- The leader answers from RAM, falling back to MySQL on a cache miss.
- Followers redirect reads to the leader, so you always see the latest acknowledged write.
- Start nodes with `--stale-reads` to let followers answer locally instead. This is faster, but a follower may lag slightly.

## Quick start

```bash
python -m venv venv
venv\Scripts\activate              # Windows  (source venv/bin/activate on Linux/macOS)
pip install -r requirements.txt
```

### 1. Single node, no database

This is the fastest way to try it out.

```bash
python main.py --no-db
python client/cli.py               # in a second terminal
```

### 2. Single node with MySQL

```bash
set DB_PASSWORD=your_mysql_password      # export DB_PASSWORD=... on Linux/macOS
python main.py --init-db                 # creates the database + tables
python main.py
```

### 3. Three-node Raft cluster

The nodes are defined in [`cluster.json`](cluster.json). Each node has a client port, a Raft port and its own database.

```bash
python scripts/run_cluster.py --init-db  # once: create + migrate the 3 databases
python scripts/run_cluster.py            # starts node1..node3 (add --no-db to skip MySQL)
python client/cli.py                     # knows every node from cluster.json
```

To watch a failover, start each node in its own terminal (`python main.py --node node1`, and so on). Stop the leader with Ctrl+C and keep using the client. A new leader takes over within a few seconds, and no acknowledged write is lost.

## Client session

```text
$ python client/cli.py
LOGIN or SIGNUP? [login]
Username: shubham
Password (hidden as you type):
Logged in as shubham on 127.0.0.1:8001
shubham@127.0.0.1:8001> SET city gurugram
(now connected to 127.0.0.1:8003)
OK
shubham@127.0.0.1:8003> GET city
gurugram
shubham@127.0.0.1:8003> INFO
node=node3 role=leader term=2 leader=node3 commit=4 applied=4 last_log=4 snapshot=0 peers=2
```

One-shot commands work too:

```bash
python client/cli.py --user shubham --password secret GET city
```

`CacheClient` can also be imported from `client/cli.py` and used from Python code.

## Protocol

Plain text over TCP, one command per line. `telnet 127.0.0.1 8001` works.

| Command | Reply |
|---|---|
| `LOGIN` / `SIGNUP` | prompts for username and password, then `READY:<user>` |
| `SET <key> <value>` | `OK` (the value may contain spaces) |
| `GET <key>` | the value, or `NULL` |
| `HAS <key>` | `1` or `0` |
| `DELETE <key>` | `OK`, or `NULL` if the key was missing |
| `INFO` | Raft status of this node |
| `QUIT` | `Bye!` |

In a cluster you may also get these replies:
- `ERROR: NOT_LEADER <host:port>`: reconnect to that address.
- `ERROR: NO_LEADER ...`: an election is in progress; retry shortly.

## Configuration

| Setting | Where | Default |
|---|---|---|
| `--node <id>` | `main.py` | none (runs a single node) |
| `--cluster <file>` | `main.py`, `scripts/run_cluster.py`, `client/cli.py` | `cluster.json` |
| `--no-db` | `main.py`, `scripts/run_cluster.py` | off (RAM only when set) |
| `--stale-reads` | `main.py`, `scripts/run_cluster.py` | off |
| `--port` / `CACHE_PORT` | `main.py` (single node) | `8001` |
| `DB_ENGINE` | env | `mysql` (`sqlite` also works) |
| `DB_NAME` | env / `cluster.json` `db_name` | `distributed_cache` |
| `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT` | env | `root`, `12345`, `127.0.0.1`, `3306` |
| `LOG_LEVEL` | env | `INFO` |

## Running the tests

```bash
pytest
```

The tests never touch your MySQL. [`tests/conftest.py`](tests/conftest.py) points Django at a throwaway SQLite file for each run.

| File | Covers |
|---|---|
| `test_cache.py` | cache, LRU eviction, thread safety |
| `test_server.py` | protocol, auth, malformed input, concurrent signup |
| `test_persistence.py` | write-through, restart, eviction fallback, startup checks |
| `test_raft.py` | election, replication, conflicts, crash and restart, snapshots |
| `test_raft_storage.py` | persisted Raft state (RAM and database) |
| `test_cluster.py` | 3 TCP servers: redirects, replication, failover, CLI |

## Project layout

```
main.py                 start a node (single or cluster)
cluster.json            cluster membership
cache/                  ICache interface, dict cache, LRU cache, factory
store/store.py          per-user store (cache + password hash)
server/tcp_server.py    TCP protocol, auth, routing writes into Raft
server/state_machine.py what Raft replicates: users + their keys (RAM + MySQL)
raft/                   RaftEngine, RPC messages, log types, storage interface
apps/users/             Django models + db_service for users and entries
apps/cluster/           Django models + storage for Raft term/vote/log
client/cli.py           interactive and one-shot client with redirect/failover
scripts/run_cluster.py  launch every node from one terminal
```

## How it was built

| Week | Milestone |
|---|---|
| 1 | Thread-safe cache, LRU eviction, cache factory |
| 2 | TCP server, SIGNUP/LOGIN, per-user isolation |
| 3 | MySQL persistence (write-through, restore on startup) |
| 4 | Raft leader election and heartbeats |
| 5 | Raft log replication, leader redirects, CLI client |
| 6 | Persisted Raft state, log compaction and snapshots |

## Limitations

- **Fixed membership.** Adding or removing nodes means editing `cluster.json` and restarting.
- **Leader reads assume a current leader.** They skip a quorum check, so a leader cut off by a network partition can briefly serve stale reads until it notices it has lost leadership.
- **Retried writes may apply twice.** If a write times out during a leader change, the client retries it. `SET` and `DELETE` are safe to repeat, but a retried `SIGNUP` may report "taken".
- **No encryption.** Node-to-node and client traffic is plain TCP, so run it on a trusted network.
- **Cluster databases start empty.** Data from an existing single-node database is not imported into a new cluster.
