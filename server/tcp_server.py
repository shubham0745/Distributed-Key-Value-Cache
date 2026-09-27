"""
server/tcp_server.py  (Week 5 — writes replicated with Raft; Week 7 extras)

Client protocol: one command per line, one response per line.

    LOGIN / SIGNUP             → prompts for username + password → READY:<user>
    ADMIN                      → prompts for the admin token → READY:admin

    SET <key> <value>          → OK             (value may contain spaces)
    SETEX <key> <secs> <value> → OK             (expires after <secs>)
    GET <key>                  → <value> | NULL
    HAS <key>                  → 1 | 0
    DELETE <key>               → OK | NULL
    EXPIRE <key> <secs>        → 1 | 0          (0: no such key)
    PERSIST <key>              → 1 | 0          (0: key had no expiry)
    TTL <key>                  → seconds left | -1 (no expiry) | -2 (no key)
    KEYS [pattern]             → <count> <key> <key> ...
    INFO                       → this node's Raft status
    MEMBERS                    → leader=<id> <id>=<client addr>/<raft addr> ...
    QUIT                       → Bye!

  Admin session (ADMIN, see config/cluster.py for the token):
    ADDNODE <id> <client host:port> <raft host:port>   → OK
    REMOVENODE <id>                                    → OK
    IMPORTUSER <username> <password_hash>              → OK | EXISTS
    IMPORTSET <username> <key> <expire_at_ms|0> <value>→ OK | EXPIRED
    MEMBERS, INFO, QUIT

Any node accepts any command. Writes go through Raft: a follower forwards
them to the leader, and the answer comes back once a majority stored and
applied them. Reads are linearizable: the node first checks with the
leader (ReadIndex) that it has applied everything committed so far.
With stale_reads=True (main.py --stale-reads) a follower skips that
check and answers from its own copy: faster, possibly a bit behind.

Every write carries a request id. Prefix a command with @<client_id>:<seq>
(client/cli.py does) and a retry of the same id is applied only once;
without a prefix the server makes one up per connection.
    ERROR: NO_LEADER ...          → election in progress, retry shortly
    ERROR: NOT_LEADER <host:port> → (ADDNODE/REMOVENODE) repeat it there
"""
import hmac
import itertools
import logging
import re
import socket
import ssl
import threading
import time
import uuid
from typing import Optional

from raft import (RaftEngine, NotLeaderError, ProposalError, ConfigChangeError,
                  MemoryRaftStorage, Member)
from server.state_machine import CacheStateMachine, now_ms

logger = logging.getLogger(__name__)

WRITE_TIMEOUT    = 5.0            # seconds to wait for a majority
READ_TIMEOUT     = 5.0            # seconds to confirm a linearizable read
ADMIN_TIMEOUT    = 60.0           # membership changes wait for catch-up
SWEEP_INTERVAL   = 0.5            # how often the leader looks for expired keys
MAX_LINE_BYTES   = 1024 * 1024    # longest command we accept (1 MB)
MAX_KEY_LENGTH   = 512            # matches CacheEntry.cache_key
MAX_TTL_SECONDS  = 10 * 365 * 24 * 3600
MIN_USERNAME_LEN = 3
MAX_USERNAME_LEN = 150            # matches CacheUser.username
MIN_PASSWORD_LEN = 4

_TAG = re.compile(r"@([A-Za-z0-9_.-]{1,64}):([0-9]{1,18})$")
_NODE_ID = re.compile(r"[A-Za-z0-9_.-]{1,64}$")
_BAD_TAG = object()
_ADMIN = object()
NO_LEADER = "ERROR: NO_LEADER election in progress, retry shortly"


class ClientConnection:
    """One connected client: buffered line reads, newline-terminated writes."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._reader = sock.makefile("rb")
        # For writes that arrive without a request tag: a session of our own
        self._client_id = uuid.uuid4().hex[:16]
        self._seq = itertools.count(1)

    def next_request_id(self) -> str:
        return f"{self._client_id}:{next(self._seq)}"

    def send(self, message: str):
        try:
            self.sock.sendall((message + "\n").encode("utf-8"))
        except OSError:
            pass

    def recv(self) -> Optional[str]:
        """Read one line (None when the client is gone or sent garbage)."""
        try:
            line = self._reader.readline(MAX_LINE_BYTES + 1)
        except (OSError, ValueError):
            return None
        if not line:
            return None
        if len(line) > MAX_LINE_BYTES:
            self.send("ERROR: Line too long")
            return None
        return line.decode("utf-8", errors="replace").strip()

    def shutdown(self):
        """Unblock a thread waiting in recv() — used when the server stops."""
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def close(self):
        try:
            self._reader.close()
        finally:
            self.sock.close()


class LocalRaftStorage(MemoryRaftStorage):
    """
    Raft state in RAM for a single MySQL-backed node. Raft threads also
    run apply()/snapshot(), which use the database, so each one closes
    its own DB connection when it finishes.
    """

    def close_thread_resources(self) -> None:
        from apps.users.db_service import close_connection
        close_connection()


class TCPServer:
    """
    Raw TCP server in front of a Raft-replicated cache.

    Single node (default):  TCPServer("127.0.0.1", 8001)
    Cluster node:           TCPServer.from_cluster_config(cluster, "node1")
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8001,
                 use_db: bool = True, *,
                 node_id: str = "standalone",
                 raft_port: Optional[int] = None,
                 peers: Optional[list[str]] = None,
                 members: Optional[list[Member]] = None,
                 join: bool = False,
                 client_addresses: Optional[dict[str, str]] = None,
                 raft_storage=None,
                 cache_capacity: int = 1000,
                 stale_reads: bool = False,
                 admin_token: Optional[str] = None,
                 ssl_context=None,
                 raft_ssl: tuple = (None, None),
                 raft_options: Optional[dict] = None):
        self.host = host
        self.port = port
        self.use_db = use_db          # set False in tests to skip MySQL
        self.node_id = node_id
        self.stale_reads = stale_reads
        self.admin_token = admin_token
        self.ssl_context = ssl_context            # TLS towards clients
        self.client_addresses = dict(client_addresses or {})
        self.state_machine = CacheStateMachine(use_db=use_db, capacity=cache_capacity)

        clustered = bool(peers) or members is not None or join
        if raft_storage is None:
            raft_storage = self._default_raft_storage(clustered)
        self.raft = RaftEngine(
            node_id=node_id,
            host=host,
            port=raft_port,
            peers=peers,
            members=members,
            join=join,
            client_address=f"{host}:{port}",
            on_become_leader=self._on_become_leader,
            on_become_follower=self._on_become_follower,
            apply_fn=self.state_machine.apply,
            snapshot_fn=self.state_machine.snapshot,
            restore_fn=self.state_machine.restore,
            storage=raft_storage,
            ssl_server_context=raft_ssl[0],
            ssl_client_context=raft_ssl[1],
            **(raft_options or {}),
        )

        self._server_socket: Optional[socket.socket] = None
        self._running = False
        self._connections: set[ClientConnection] = set()
        self._connections_lock = threading.Lock()

    @classmethod
    def from_cluster_config(cls, cluster, node_id: str, use_db: bool = True,
                            join: bool = False, **kwargs):
        node = cluster.get(node_id)
        return cls(
            host=node.host,
            port=node.client_port,
            use_db=use_db,
            node_id=node.id,
            raft_port=node.raft_port,
            members=cluster.members(),
            join=join,
            client_addresses=cluster.client_addresses(),
            admin_token=cluster.admin_token,
            ssl_context=cluster.client_port_context(node.id),
            raft_ssl=(cluster.raft_server_context(node.id),
                      cluster.raft_client_context(node.id)),
            **kwargs,
        )

    def _default_raft_storage(self, clustered: bool):
        """
        A lone node has nobody to replicate to and its data is already
        durable in MySQL, so its Raft log can live in RAM. A cluster node
        must remember term/vote/log/membership across restarts
        (see raft/storage.py).
        """
        if self.use_db and clustered:
            from apps.cluster.raft_storage import DjangoRaftStorage
            return DjangoRaftStorage(self.node_id)
        if self.use_db:
            return LocalRaftStorage()
        return MemoryRaftStorage()

    @property
    def _stores(self):
        """username → Store (kept for older tests and debugging)."""
        return self.state_machine.stores

    # ──────────────────────────────────────────────
    # SERVER LIFECYCLE
    # ──────────────────────────────────────────────

    def start(self):
        if self.use_db:
            self._load_from_db()

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(50)
        # A timeout lets Ctrl+C through — a blocking accept() on Windows
        # ignores it until the next client connects.
        self._server_socket.settimeout(1.0)
        self._running = True

        try:
            self.raft.start()
        except OSError:
            self.stop()
            raise

        threading.Thread(target=self._expiry_loop, daemon=True,
                         name=f"expiry-{self.node_id}").start()
        logger.info(f"Server '{self.node_id}' started on {self.host}:{self.port}"
                    f"{' (TLS)' if self.ssl_context else ''}")
        self._accept_connections()

    def stop(self, graceful: bool = False):
        """
        Stop serving. graceful=True first hands leadership to another
        node, so the cluster keeps going without waiting for an election.
        """
        if graceful and self._running and self.raft.is_leader():
            logger.info("Handing leadership to another node before stopping...")
            self.raft.transfer_leadership()
        self._running = False
        self.raft.stop()
        if self._server_socket:
            try:
                # On Linux close() alone doesn't wake the thread blocked in
                # accept(), so the port stays taken until it times out.
                self._server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass            # Windows refuses shutdown on a listening socket
            try:
                self._server_socket.close()
            except Exception:
                pass
        # Drop connected clients too, so they fail over to another node
        with self._connections_lock:
            connections = list(self._connections)
        for conn in connections:
            conn.shutdown()

    def _load_from_db(self):
        """
        On startup: read all users + their cache entries from MySQL
        and rebuild the in-memory stores.

        Without this, every restart loses all data.
        With this, the server is stateful across restarts.
        """
        self.state_machine.load_from_db()

    def _accept_connections(self):
        while self._running:
            try:
                client_socket, address = self._server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            client_socket.settimeout(None)
            logger.info(f"New connection from {address}")
            threading.Thread(
                target=self._handle_client,
                args=(client_socket, address),
                daemon=True,
                name=f"client-{address[1]}"
            ).start()

    def _expiry_loop(self):
        """Leader only: once a key's expiry passes, log a SWEEP to delete it everywhere."""
        while self._running:
            time.sleep(SWEEP_INTERVAL)
            if not self.raft.is_leader():
                continue
            try:
                first = self.state_machine.next_expiry()
                now = now_ms()
                if first is not None and first <= now:
                    removed = self.raft.submit(f"SWEEP {now}", "", timeout=WRITE_TIMEOUT)
                    logger.debug(f"Expired {removed} key(s)")
            except Exception as e:
                logger.debug(f"Expiry sweep skipped: {e}")
        if self.use_db:
            from apps.users.db_service import close_connection
            close_connection()

    # ──────────────────────────────────────────────
    # CLIENT HANDLING
    # ──────────────────────────────────────────────

    def _handle_client(self, client_socket: socket.socket, address: tuple):
        if self.ssl_context:
            try:
                client_socket.settimeout(10)
                client_socket = self.ssl_context.wrap_socket(client_socket, server_side=True)
                client_socket.settimeout(None)
            except (ssl.SSLError, OSError) as e:
                logger.info(f"TLS handshake with {address} failed: {e}")
                client_socket.close()
                return

        conn = ClientConnection(client_socket)
        with self._connections_lock:
            self._connections.add(conn)
        try:
            conn.send("Welcome! Type LOGIN or SIGNUP")
            principal = self._handle_auth(conn)
            if principal is None:
                return
            if principal is _ADMIN:
                conn.send("READY:admin")
                self._handle_admin_commands(conn)
            else:
                conn.send(f"READY:{principal}")
                self._handle_commands(conn, principal)
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
            logger.info(f"Client {address} disconnected abruptly")
        except Exception:
            logger.exception(f"Error for {address}")
        finally:
            with self._connections_lock:
                self._connections.discard(conn)
            conn.close()
            if self.use_db:
                from apps.users.db_service import close_connection
                close_connection()

    # ──────────────────────────────────────────────
    # AUTH
    # ──────────────────────────────────────────────

    def _handle_auth(self, conn: ClientConnection):
        while True:
            line = conn.recv()
            if line is None:
                return None
            tag, line = _split_tag(line)
            if tag is _BAD_TAG:
                conn.send("ERROR: Bad request tag (use @client_id:seq)")
                continue
            auth_type = line.upper()
            if auth_type == "LOGIN":
                principal = self._do_login(conn)
            elif auth_type == "SIGNUP":
                principal = self._do_signup(conn, tag)
            elif auth_type == "ADMIN":
                principal = _ADMIN if self._do_admin(conn) else None
            elif auth_type == "QUIT":
                conn.send("Bye!")
                return None
            else:
                conn.send("ERROR: Type LOGIN or SIGNUP")
                continue
            if principal:
                return principal

    def _do_login(self, conn: ClientConnection) -> Optional[str]:
        conn.send("Username:")
        username = conn.recv()
        if username is None:
            return None

        if not self.state_machine.user_exists(username) and not self.stale_reads:
            # We may just not have applied their SIGNUP yet — catch up with
            # the leader before saying "not found". ("Found" is always
            # right: accounts never change once created.)
            if not self._read_barrier(conn):
                return None
        if not self.state_machine.user_exists(username):
            conn.send("ERROR: User not found. Try SIGNUP")
            return None

        conn.send("Password:")
        password = conn.recv()
        if password is None:
            return None

        if self.state_machine.verify_password(username, password):
            return username
        conn.send("ERROR: Wrong password")
        return None

    def _do_signup(self, conn: ClientConnection, tag: Optional[str]) -> Optional[str]:
        conn.send("Choose username:")
        username = conn.recv()
        if username is None:
            return None

        error = _validate_username(username)
        if error:
            conn.send(f"ERROR: {error}")
            return None
        if self.state_machine.user_exists(username):
            # A retry whose first attempt DID create the account (the reply
            # was lost): the session table remembers — finish the login.
            if tag and self.state_machine.session_result(tag) is True:
                conn.send("Choose password:")
                password = conn.recv()
                if password is not None and self.state_machine.verify_password(username, password):
                    return username
                conn.send("ERROR: Wrong password")
                return None
            conn.send("ERROR: Username already taken")
            return None

        conn.send("Choose password:")
        password = conn.recv()
        if password is None:
            return None

        if len(password) < MIN_PASSWORD_LEN:
            conn.send(f"ERROR: Password must be at least {MIN_PASSWORD_LEN} characters")
            return None

        # Hash once, here; every node stores the same hash.
        from django.contrib.auth.hashers import make_password
        ok, created = self._submit(conn, username, f"SIGNUP {make_password(password)}", tag)
        if not ok:
            return None
        if not created:
            conn.send("ERROR: Username already taken")
            return None
        return username

    def _do_admin(self, conn: ClientConnection) -> bool:
        conn.send("Token:")
        token = conn.recv()
        if token is None:
            return False
        if not self.admin_token:
            conn.send("ERROR: Admin access is disabled (set CACHE_ADMIN_TOKEN)")
            return False
        if not hmac.compare_digest(token.encode(), self.admin_token.encode()):
            conn.send("ERROR: Wrong token")
            return False
        return True

    # ──────────────────────────────────────────────
    # USER COMMANDS
    # ──────────────────────────────────────────────

    def _handle_commands(self, conn: ClientConnection, username: str):
        handlers = {
            "SET":     self._cmd_set,
            "SETEX":   self._cmd_setex,
            "GET":     self._cmd_get,
            "HAS":     self._cmd_has,
            "DELETE":  self._cmd_delete,
            "EXPIRE":  self._cmd_expire,
            "PERSIST": self._cmd_persist,
            "TTL":     self._cmd_ttl,
            "KEYS":    self._cmd_keys,
            "INFO":    self._cmd_info,
            "MEMBERS": self._cmd_members,
        }
        self._dispatch(conn, username, handlers,
                       "Use SET/SETEX/GET/HAS/DELETE/EXPIRE/PERSIST/TTL/KEYS/INFO/MEMBERS/QUIT")

    def _dispatch(self, conn: ClientConnection, principal, handlers: dict, usage: str):
        while True:
            raw = conn.recv()
            if raw is None:
                break
            tag, raw = _split_tag(raw)
            if tag is _BAD_TAG:
                conn.send("ERROR: Bad request tag (use @client_id:seq)")
                continue
            if not raw:
                continue
            # split(None, 1) tolerates repeated spaces between the parts
            # while keeping spaces INSIDE the value: SET k hello  world
            parts = raw.split(None, 1)
            command = parts[0].upper()
            args = parts[1] if len(parts) > 1 else ""

            if command == "QUIT":
                conn.send("Bye!")
                break
            handler = handlers.get(command)
            if handler is None:
                conn.send(f"ERROR: Unknown command. {usage}")
                continue
            try:
                handler(conn, principal, args, tag)
            except Exception as e:
                # One bad command must not end the whole session
                logger.exception(f"{command} failed")
                conn.send(f"ERROR: {command} failed: {e}")

    def _cmd_set(self, conn, username, args, tag):
        parts = args.split(None, 1)
        if len(parts) != 2:
            conn.send("ERROR: Usage: SET <key> <value>")
            return
        key, value = parts
        if self._valid_key(conn, key):
            ok, _ = self._submit(conn, username, f"SET {key} {value}", tag)
            if ok:
                conn.send("OK")

    def _cmd_setex(self, conn, username, args, tag):
        parts = args.split(None, 2)
        seconds = _seconds(parts[1]) if len(parts) == 3 else None
        if seconds is None:
            conn.send(f"ERROR: Usage: SETEX <key> <seconds 1..{MAX_TTL_SECONDS}> <value>")
            return
        key, _, value = parts
        if self._valid_key(conn, key):
            expire_at = now_ms() + seconds * 1000
            ok, _ = self._submit(conn, username, f"SETEX {key} {expire_at} {value}", tag)
            if ok:
                conn.send("OK")

    def _cmd_get(self, conn, username, args, tag):
        parts = args.split()
        if len(parts) != 1:
            conn.send("ERROR: Usage: GET <key>")
            return
        if self._read_barrier(conn):
            value = self.state_machine.get(username, parts[0])
            conn.send(value if value is not None else "NULL")

    def _cmd_has(self, conn, username, args, tag):
        parts = args.split()
        if len(parts) != 1:
            conn.send("ERROR: Usage: HAS <key>")
            return
        if self._read_barrier(conn):
            conn.send("1" if self.state_machine.has(username, parts[0]) else "0")

    def _cmd_delete(self, conn, username, args, tag):
        parts = args.split()
        if len(parts) != 1:
            conn.send("ERROR: Usage: DELETE <key>")
            return
        ok, existed = self._submit(conn, username, f"DELETE {parts[0]} {now_ms()}", tag)
        if ok:
            conn.send("OK" if existed else "NULL")

    def _cmd_expire(self, conn, username, args, tag):
        parts = args.split()
        seconds = _seconds(parts[1]) if len(parts) == 2 else None
        if seconds is None:
            conn.send(f"ERROR: Usage: EXPIRE <key> <seconds 1..{MAX_TTL_SECONDS}>")
            return
        now = now_ms()
        ok, done = self._submit(conn, username,
                                f"EXPIREAT {parts[0]} {now + seconds * 1000} {now}", tag)
        if ok:
            conn.send("1" if done else "0")

    def _cmd_persist(self, conn, username, args, tag):
        parts = args.split()
        if len(parts) != 1:
            conn.send("ERROR: Usage: PERSIST <key>")
            return
        ok, done = self._submit(conn, username, f"PERSIST {parts[0]} {now_ms()}", tag)
        if ok:
            conn.send("1" if done else "0")

    def _cmd_ttl(self, conn, username, args, tag):
        parts = args.split()
        if len(parts) != 1:
            conn.send("ERROR: Usage: TTL <key>")
            return
        if self._read_barrier(conn):
            conn.send(str(self.state_machine.ttl(username, parts[0])))

    def _cmd_keys(self, conn, username, args, tag):
        parts = args.split()
        if len(parts) > 1:
            conn.send("ERROR: Usage: KEYS [pattern]")
            return
        if self._read_barrier(conn):
            keys = self.state_machine.keys(username, parts[0] if parts else None)
            conn.send(" ".join([str(len(keys))] + keys))

    def _cmd_info(self, conn, principal, args, tag):
        status = self.raft.status()
        conn.send(" ".join(f"{k}={v}" for k, v in status.items()))

    def _cmd_members(self, conn, principal, args, tag):
        parts = [f"leader={self.raft.get_leader() or '-'}"]
        for m in self.raft.members():
            client = m.client_address or self.client_addresses.get(m.node_id, "")
            parts.append(f"{m.node_id}={client or '-'}/{m.raft_address or '-'}")
        conn.send(" ".join(parts))

    # ──────────────────────────────────────────────
    # ADMIN COMMANDS
    # ──────────────────────────────────────────────

    def _handle_admin_commands(self, conn: ClientConnection):
        handlers = {
            "ADDNODE":    self._admin_add_node,
            "REMOVENODE": self._admin_remove_node,
            "IMPORTUSER": self._admin_import_user,
            "IMPORTSET":  self._admin_import_set,
            "MEMBERS":    self._cmd_members,
            "INFO":       self._cmd_info,
        }
        self._dispatch(conn, _ADMIN, handlers,
                       "Use ADDNODE/REMOVENODE/IMPORTUSER/IMPORTSET/MEMBERS/INFO/QUIT")

    def _admin_add_node(self, conn, _, args, tag):
        parts = args.split()
        if (len(parts) != 3 or not _NODE_ID.match(parts[0]) or
                not _is_address(parts[1]) or not _is_address(parts[2])):
            conn.send("ERROR: Usage: ADDNODE <id> <client host:port> <raft host:port>")
            return
        node_id, client, raft_address = parts
        self._membership_change(conn, lambda: self.raft.add_member(
            Member(node_id, raft_address, client), timeout=ADMIN_TIMEOUT))

    def _admin_remove_node(self, conn, _, args, tag):
        parts = args.split()
        if len(parts) != 1:
            conn.send("ERROR: Usage: REMOVENODE <id>")
            return
        self._membership_change(conn, lambda: self.raft.remove_member(
            parts[0], timeout=ADMIN_TIMEOUT))

    def _membership_change(self, conn, change):
        try:
            change()
            conn.send("OK")
        except NotLeaderError as e:
            conn.send(self._not_leader_message(e.leader_id or self.raft.get_leader()))
        except (ConfigChangeError, ProposalError) as e:
            conn.send(f"ERROR: {e}")

    def _admin_import_user(self, conn, _, args, tag):
        parts = args.split()
        if len(parts) != 2 or _validate_username(parts[0]):
            conn.send("ERROR: Usage: IMPORTUSER <username> <password_hash>")
            return
        ok, created = self._submit(conn, parts[0], f"SIGNUP {parts[1]}", tag)
        if ok:
            conn.send("OK" if created else "EXISTS")

    def _admin_import_set(self, conn, _, args, tag):
        parts = args.split(None, 3)
        if len(parts) != 4 or not parts[2].isdigit():
            conn.send("ERROR: Usage: IMPORTSET <username> <key> <expire_at_ms or 0> <value>")
            return
        username, key, expire_at, value = parts
        if not self._valid_key(conn, key):
            return
        expire_at = int(expire_at)
        if expire_at and expire_at <= now_ms():
            conn.send("EXPIRED")
            return
        command = f"SETEX {key} {expire_at} {value}" if expire_at else f"SET {key} {value}"
        ok, stored = self._submit(conn, username, command, tag)
        if ok:
            conn.send("OK" if stored else "ERROR: Unknown user")

    # ──────────────────────────────────────────────
    # RAFT GLUE
    # ──────────────────────────────────────────────

    def _submit(self, conn: ClientConnection, username: str, command: str,
                tag: Optional[str]):
        """
        Push one write through Raft (forwarded to the leader if needed).
        Returns (True, apply result) once it is committed and applied, or
        (False, None) after telling the client why not.
        """
        request_id = tag or conn.next_request_id()
        try:
            return True, self.raft.submit(command, username, request_id=request_id,
                                          timeout=WRITE_TIMEOUT)
        except NotLeaderError:
            conn.send(NO_LEADER)
        except ProposalError as e:
            conn.send(f"ERROR: {e}")
        return False, None

    def _read_barrier(self, conn: ClientConnection) -> bool:
        """Make the next local read linearizable (unless stale reads are allowed)."""
        if self.stale_reads:
            return True
        try:
            self.raft.read_index(timeout=READ_TIMEOUT)
            return True
        except NotLeaderError:
            conn.send(NO_LEADER)
        except ProposalError as e:
            conn.send(f"ERROR: {e}")
        return False

    def _valid_key(self, conn: ClientConnection, key: str) -> bool:
        if len(key) > MAX_KEY_LENGTH:
            conn.send(f"ERROR: Key longer than {MAX_KEY_LENGTH} characters")
            return False
        return True

    def _not_leader_message(self, leader_id: Optional[str]) -> str:
        member = self.raft.member(leader_id) if leader_id else None
        address = (member.client_address if member and member.client_address
                   else self.client_addresses.get(leader_id))
        if address:
            return f"ERROR: NOT_LEADER {address}"
        return NO_LEADER

    def _on_become_leader(self):
        logger.info(f"Node '{self.node_id}' is now the LEADER, accepting writes")

    def _on_become_follower(self):
        logger.info(f"Node '{self.node_id}' stepped down, forwarding writes to the new leader")


def _split_tag(line: str):
    """"@client:seq REST" → ("client:seq", "REST"); no tag → (None, line)."""
    if not line.startswith("@"):
        return None, line
    head, _, rest = line.partition(" ")
    match = _TAG.match(head)
    if not match:
        return _BAD_TAG, rest.strip()
    return f"{match.group(1)}:{match.group(2)}", rest.strip()


def _seconds(text: str) -> Optional[int]:
    if not text.isdigit():
        return None
    value = int(text)
    return value if 1 <= value <= MAX_TTL_SECONDS else None


def _is_address(text: str) -> bool:
    host, _, port = text.rpartition(":")
    return bool(host) and port.isdigit() and 0 < int(port) < 65536


def _validate_username(username: str) -> Optional[str]:
    if len(username) < MIN_USERNAME_LEN:
        return f"Username must be at least {MIN_USERNAME_LEN} characters"
    if len(username) > MAX_USERNAME_LEN:
        return f"Username must be at most {MAX_USERNAME_LEN} characters"
    if any(ch.isspace() for ch in username):
        return "Username cannot contain spaces"
    return None
