# Distributed Key-Value Cache

![tests](https://github.com/shubham0745/Distributed-Key-Value-Cache/actions/workflows/tests.yml/badge.svg)

A Redis-style key-value cache written from scratch in Python. Several nodes stay in sync with the **Raft** consensus algorithm, and every node keeps a durable copy in **MySQL**. There is no consensus library: leader election, log replication, persistence, snapshots, membership changes and linearizable reads are all implemented here.

**Author:** Shubham Kumar

## Features

- **Cache**
  - Isolated, thread-safe LRU cache per user, backed by MySQL: evicted keys are read back, never lost.
  - `SETEX` / `EXPIRE` / `TTL` / `PERSIST` / `KEYS [pattern]`, with expiry that is identical on every node.
- **Accounts:** `SIGNUP` / `LOGIN` with PBKDF2-hashed passwords, plus a token-protected admin session.
- **Raft**
  - Leader election with **Pre-Vote**, leader stickiness and **CheckQuorum**: a node that loses contact can't disrupt the cluster when it returns, and a cut-off leader steps down.
  - Log replication with the consistency check and fast conflict back-off.
  - Term, vote, log and membership are persisted.
  - Log compaction, with **snapshots streamed in chunks** to followers that fall behind.
  - **Membership changes at runtime**: add or remove nodes one at a time; new nodes catch up as learners first.
  - **Leadership transfer**: a gracefully stopped leader hands over in milliseconds.
- **Consistency:** any node serves any command. Writes are forwarded to the leader, and reads are linearizable via **ReadIndex**. `--stale-reads` trades that for speed.
- **Exactly-once writes:** every request carries a client id and sequence number, so a retried write is applied only once.
- **Security:** TLS for clients, **mutual TLS** between nodes, and a certificate generator.
- **Tooling:** a CLI client with failover and retries, a cluster launcher, and an import tool for existing databases.
- **Tests:** 260 covering unit behaviour, real multi-node clusters over TCP, network partitions, crash and restart, membership changes, TLS and snapshots. CI runs them on Linux and Windows.

## Architecture

```
             client/cli.py  or  telnet  (TLS optional)
                     │  one command per line
                     ▼
   ┌──────────────────── node (main.py) ─────────────────────┐
   │ server/tcp_server.py   auth, commands, admin session     │
   │     │ writes: submit()           │ reads: read_index()   │
   │     ▼                            ▼                       │
   │ raft/node.py (RaftEngine) ──commit──▶ server/state_machine.py
   │   election · replication ·  apply()   per-user LRU + expiry
   │   snapshots · membership ·            sessions (exactly-once)
   │   ReadIndex · forwarding                │ write-through    │
   │     │ raft_meta / raft_log              ▼                  │
   │     └────────────────────────▶ MySQL (this node's own DB)  │
   └──────┬───────────────────────────────────────────────────┘
          │ RequestVote · AppendEntries · InstallSnapshot · ReadIndex
          │ Forward · TimeoutNow      (JSON over TCP, mutual TLS optional)
          ▼
     other nodes: same layout, each with its own database
```

**Write path** (`SET`, `SETEX`, `DELETE`, `EXPIRE`, `PERSIST`, `SIGNUP`)
1. Any node receives the command. A follower forwards it to the leader.
2. The leader appends it to the Raft log and replicates it.
3. Once a majority stores it, it is committed.
4. Every node applies committed entries in log order: MySQL first, then RAM.
5. The client gets its answer after the leader has applied the entry.

**Read path** (`GET`, `HAS`, `TTL`, `KEYS`)
- The node first gets the leader's commit index. The leader confirms that index with a heartbeat round to a majority.
- The node waits until it has applied up to that index, then answers from its own copy.
- The answer always reflects every write acknowledged before the read started, whichever node you ask.

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

```powershell
$env:DB_PASSWORD = "your_mysql_password"   # export DB_PASSWORD=... on Linux/macOS
python main.py --init-db                   # creates the database + tables
python main.py
```

### 3. Three-node Raft cluster

The nodes are defined in [`cluster.json`](cluster.json). Each node has a client port, a Raft port and its own database.

```bash
python scripts/run_cluster.py --init-db  # once: create + migrate each node's database
python scripts/run_cluster.py            # starts node1..node3 (add --no-db to skip MySQL)
python client/cli.py                     # knows every node from cluster.json
```

To watch a failover, start each node in its own terminal (`python main.py --node node1`, and so on). Kill the leader's terminal: another node takes over within an election timeout, and the client carries on. Stop it with **Ctrl+C** instead and it hands leadership over first, so the switch takes milliseconds.

## Using the client

```text
$ python client/cli.py
LOGIN or SIGNUP? [login]
Username: shubham
Password (hidden as you type):
Logged in as shubham on 127.0.0.1:8001
shubham@127.0.0.1:8001> SET city gurugram
OK
shubham@127.0.0.1:8001> SETEX otp 60 4711
OK
shubham@127.0.0.1:8001> TTL otp
60
shubham@127.0.0.1:8001> KEYS
2 city otp
shubham@127.0.0.1:8001> MEMBERS
leader=node2 node1=127.0.0.1:8001/127.0.0.1:9001 node2=127.0.0.1:8002/127.0.0.1:9002 node3=127.0.0.1:8003/127.0.0.1:9003
```

- **One-shot commands:** `python client/cli.py --user shubham --password secret GET city`
- **Options:**
  - `--nodes host:port,...` or `--cluster file`: which nodes to use. The default is every node in `cluster.json`.
  - `--signup`: create the account instead of logging in.
  - `--admin` (with `--admin-token`): open an admin session.
  - `--ca ca.pem`: the CA to trust for TLS.
- **From Python:** `CacheClient` in `client/cli.py` provides `set`, `setex`, `get`, `ttl`, `keys` and the rest.

## Operating a cluster

Admin commands need a token. Set it for the nodes and the client, or put `"admin_token"` in `cluster.json` (not in a public repo):

```powershell
$env:CACHE_ADMIN_TOKEN = "choose-a-long-secret"      # export CACHE_ADMIN_TOKEN=... on Linux/macOS
```

**Add a node while the cluster runs.**
1. Add it to `cluster.json` with `"join": true`, and create its database with `python main.py --node node4 --init-db`.
2. Start it with `python main.py --node node4`.

It asks the cluster to add it, catches up as a learner, then becomes a voting member. From then on the membership lives in the Raft log, and `cluster.json` only describes the starting membership.

**Remove a node.**

```bash
python client/cli.py --admin REMOVENODE node3
```

Then stop that node's process. Removing the current leader works too: it steps down once the change is committed.

**See the cluster.** `python client/cli.py --admin MEMBERS`, or `INFO` for one node's Raft status.

**Encrypt everything.**

```bash
python scripts/gen_certs.py --enable     # CA + one certificate per node; adds "tls" to cluster.json
```

Then restart the nodes. Clients verify the nodes with `certs/ca.pem`, and `client/cli.py` picks it up from `cluster.json`. Nodes authenticate each other with their certificates, so a machine without one can't join the Raft traffic. For a single node, use `python main.py --tls-cert cert.pem --tls-key key.pem` and `client/cli.py --ca ca.pem`.

**Bring existing data into a cluster.** Start the cluster, then:

```bash
python scripts/import_data.py --source-db distributed_cache      # or --source-sqlite old.sqlite3
```

Users keep their passwords, keys keep their expiry, and the import is safe to run twice.

## Protocol

Plain text over TCP, one command per line, so `telnet 127.0.0.1 8001` works.

| Command | Reply |
|---|---|
| `LOGIN` / `SIGNUP` | prompts for username and password, then `READY:<user>` |
| `ADMIN` | prompts for the admin token, then `READY:admin` |
| `SET <key> <value>` | `OK` (the value may contain spaces; clears any expiry) |
| `SETEX <key> <seconds> <value>` | `OK` |
| `GET <key>` | the value, or `NULL` |
| `HAS <key>` | `1` or `0` |
| `DELETE <key>` | `OK`, or `NULL` if the key was missing |
| `EXPIRE <key> <seconds>` | `1`, or `0` if there is no such key |
| `PERSIST <key>` | `1`, or `0` if the key had no expiry |
| `TTL <key>` | seconds left; `-1` means no expiry, `-2` means no such key |
| `KEYS [pattern]` | `<count> <key> <key> ...` (glob pattern, e.g. `user:*`) |
| `INFO` | this node's Raft status |
| `MEMBERS` | `leader=<id> <id>=<client addr>/<raft addr> ...` |
| `QUIT` | `Bye!` |
| **Admin session** | |
| `ADDNODE <id> <client host:port> <raft host:port>` | `OK` |
| `REMOVENODE <id>` | `OK` |
| `IMPORTUSER <username> <password_hash>` | `OK` or `EXISTS` |
| `IMPORTSET <username> <key> <expire_at_ms or 0> <value>` | `OK` or `EXPIRED` |

**Exactly-once writes.** Prefix any command with `@<client_id>:<seq>` (the CLI does this). If a reply is lost and you resend the same prefix, the write isn't applied a second time; you get the original answer.

**Other replies.**
- `ERROR: NO_LEADER ...`: an election is running; retry shortly. The CLI does this for you.
- `ERROR: NOT_LEADER <host:port>`: only `ADDNODE` and `REMOVENODE` must run on the leader. The CLI follows this redirect.

## Configuration

| Setting | Where | Default |
|---|---|---|
| `--node <id>` | `main.py` | none (runs a single node) |
| `--cluster <file>` | `main.py`, scripts, `client/cli.py` | `cluster.json` |
| `--join` / `"join": true` | `main.py` / `cluster.json` | off |
| `--no-db` | `main.py`, `scripts/run_cluster.py` | off (RAM only when set) |
| `--stale-reads` | `main.py`, `scripts/run_cluster.py` | off |
| `--host` | `main.py` (single node) | `127.0.0.1` |
| `--port` / `CACHE_PORT` | `main.py` (single node) | `8001` |
| `--tls-cert`, `--tls-key` | `main.py` (single node) | plaintext |
| `CACHE_ADMIN_TOKEN` / `"admin_token"` | env / `cluster.json` | admin disabled |
| `"tls": {"ca", "cert_dir"}` | `cluster.json` | plaintext |
| `DB_ENGINE` | env | `mysql` (`sqlite` also works) |
| `DB_NAME` | env / `cluster.json` `db_name` | `distributed_cache` |
| `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT` | env | `root`, `12345`, `127.0.0.1`, `3306` |
| `LOG_LEVEL` | env | `INFO` |

**Upgrading from an earlier version:** stop the nodes, then run `python scripts/run_cluster.py --init-db` (cluster) or `python main.py --init-db` (single node) to add the new tables and columns. Existing data, users and Raft logs are kept.

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
| `test_state_machine.py` | deterministic expiry, exactly-once sessions, snapshots (RAM and database) |
| `test_raft.py` | election, replication, conflicts, crash and restart, snapshots |
| `test_raft_extensions.py` | Pre-Vote, CheckQuorum, partitions, ReadIndex, forwarding, membership, leadership transfer, chunked snapshots |
| `test_raft_storage.py` | persisted Raft state (RAM and database) |
| `test_cluster.py` | real TCP clusters: any-node access, failover, hand-over, expiry, admin, CLI |
| `test_tls.py` | TLS, mutual TLS, certificate generation |
| `test_import.py` | importing an existing database |
| `test_config.py` | `cluster.json` validation |

## Project layout

```
main.py                   start a node (single or cluster, --join, --init-db)
cluster.json              starting membership, ports, databases, TLS
cache/                    ICache interface, dict cache, LRU cache, factory
store/store.py            per-user store: cache, expiry times, password hash
server/tcp_server.py      protocol, auth, admin session, expiry sweeper
server/state_machine.py   what Raft replicates: users, keys, expiry, sessions
raft/                     RaftEngine, RPC messages, log types, storage interface
apps/users/               Django models + db_service for users, entries, sessions
apps/cluster/             Django models + storage for Raft term/vote/log/membership
config/                   settings, cluster.json loader, TLS contexts
client/cli.py             client with failover, retries, redirects, TLS
scripts/                  run_cluster.py, gen_certs.py, import_data.py
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
| 7 | Pre-Vote, CheckQuorum, ReadIndex, write forwarding, membership changes, leadership transfer, exactly-once writes, expiry, TLS, import, CI |

## Design trade-offs

These are deliberate choices that come with any Raft-based system, not missing pieces.

- **Needs a majority of nodes.** Writes and linearizable reads need a majority alive (2 of 3, 3 of 5). Without one the cluster refuses rather than risk inconsistent data, so it picks consistency over availability. `--stale-reads` keeps reads flowing from any live node.
- **Reads cost a heartbeat round.** Confirming leadership with a majority takes about one heartbeat round per read. That is the price of linearizability, and `--stale-reads` skips it.
- **Expiry follows the clocks.** Expiry times are stamped by the node that receives the command. A key can expire slightly earlier or later if node clocks differ. Keep clocks in sync with NTP, as with any distributed TTL.
- **One membership change at a time.** Adding or removing one node per change keeps old and new majorities overlapping. That is what makes changes safe without a joint-consensus phase.
- **Snapshots briefly pause applying.** While a snapshot is written for a lagging follower, the sending node pauses applying new entries. The dump streams through a file, so its size isn't limited by RAM.
