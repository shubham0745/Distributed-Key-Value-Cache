"""
server/tcp_server.py  (Week 5 — writes replicated with Raft)

Client protocol: one command per line, one response per line.

    LOGIN / SIGNUP        → prompts for username + password → READY:<user>
    SET <key> <value>     → OK             (value may contain spaces)
    GET <key>             → <value> | NULL
    HAS <key>             → 1 | 0
    DELETE <key>          → OK | NULL
    INFO                  → this node's Raft status
    QUIT                  → Bye!

Writes (SIGNUP, SET, DELETE) go through Raft: the leader logs them,
replicates them to a majority, applies them, and only THEN answers OK.
Reads (GET, HAS) are also answered by the leader, so you always see
the latest acknowledged write. On a follower they are answered with

    ERROR: NOT_LEADER <host:port>     → reconnect there and repeat
    ERROR: NO_LEADER ...              → election running, retry shortly

(client/cli.py follows these for you.) With stale_reads=True
(main.py --stale-reads) followers answer GET/HAS from their own copy
instead: faster and spreads the load, but may lag the leader a little.
LOGIN and INFO work on every node.

Changes from Week 3:
  - Writes are Raft proposals; server/state_machine.py applies them
  - One malformed command answers ERROR instead of dropping the client
  - Concurrent SIGNUPs for one name: first in the Raft log wins
  - Startup refuses to run on top of a database it couldn't read
"""
import socket
import threading
import logging
from typing import Optional

from raft import RaftEngine, NotLeaderError, ProposalError, MemoryRaftStorage
from server.state_machine import CacheStateMachine

logger = logging.getLogger(__name__)

WRITE_TIMEOUT    = 5.0            # seconds to wait for a majority
MAX_LINE_BYTES   = 1024 * 1024    # longest command we accept (1 MB)
MAX_KEY_LENGTH   = 512            # matches CacheEntry.cache_key
MIN_USERNAME_LEN = 3
MAX_USERNAME_LEN = 150            # matches CacheUser.username
MIN_PASSWORD_LEN = 4


class ClientConnection:
    """One connected client: buffered line reads, newline-terminated writes."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._reader = sock.makefile("rb")

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
                 client_addresses: Optional[dict[str, str]] = None,
                 raft_storage=None,
                 cache_capacity: int = 1000,
                 stale_reads: bool = False,
                 raft_options: Optional[dict] = None):
        self.host = host
        self.port = port
        self.use_db = use_db          # set False in tests to skip MySQL
        self.node_id = node_id
        self.stale_reads = stale_reads
        self.client_addresses = dict(client_addresses or {})
        self.state_machine = CacheStateMachine(use_db=use_db, capacity=cache_capacity)

        peers = list(peers or [])
        if raft_storage is None:
            raft_storage = self._default_raft_storage(clustered=bool(peers))
        self.raft = RaftEngine(
            node_id=node_id,
            host=host,
            port=raft_port,
            peers=peers,
            on_become_leader=self._on_become_leader,
            on_become_follower=self._on_become_follower,
            apply_fn=self.state_machine.apply,
            snapshot_fn=self.state_machine.snapshot,
            restore_fn=self.state_machine.restore,
            storage=raft_storage,
            **(raft_options or {}),
        )

        self._server_socket: Optional[socket.socket] = None
        self._running = False
        self._connections: set[ClientConnection] = set()
        self._connections_lock = threading.Lock()

    @classmethod
    def from_cluster_config(cls, cluster, node_id: str, use_db: bool = True, **kwargs):
        node = cluster.get(node_id)
        return cls(
            host=node.host,
            port=node.client_port,
            use_db=use_db,
            node_id=node.id,
            raft_port=node.raft_port,
            peers=cluster.peers_of(node.id),
            client_addresses=cluster.client_addresses(),
            **kwargs,
        )

    def _default_raft_storage(self, clustered: bool):
        """
        A lone node has nobody to replicate to and its data is already
        durable in MySQL, so its Raft log can live in RAM. A cluster node
        must remember term/vote/log across restarts (see raft/storage.py).
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

        logger.info(f"Server '{self.node_id}' started on {self.host}:{self.port}")
        self._accept_connections()

    def stop(self):
        self._running = False
        self.raft.stop()
        if self._server_socket:
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

    # ──────────────────────────────────────────────
    # CLIENT HANDLING
    # ──────────────────────────────────────────────

    def _handle_client(self, client_socket: socket.socket, address: tuple):
        conn = ClientConnection(client_socket)
        with self._connections_lock:
            self._connections.add(conn)
        try:
            conn.send("Welcome! Type LOGIN or SIGNUP")
            current_user = self._handle_auth(conn)
            if current_user is None:
                return
            conn.send(f"READY:{current_user}")
            self._handle_commands(conn, current_user)
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

    def _handle_auth(self, conn: ClientConnection) -> Optional[str]:
        while True:
            auth_type = conn.recv()
            if auth_type is None:
                return None
            auth_type = auth_type.upper()
            if auth_type == "LOGIN":
                username = self._do_login(conn)
            elif auth_type == "SIGNUP":
                username = self._do_signup(conn)
            elif auth_type == "QUIT":
                conn.send("Bye!")
                return None
            else:
                conn.send("ERROR: Type LOGIN or SIGNUP")
                continue
            if username:
                return username

    def _do_login(self, conn: ClientConnection) -> Optional[str]:
        conn.send("Username:")
        username = conn.recv()
        if username is None:
            return None

        if not self.state_machine.user_exists(username):
            if not self.stale_reads and not self.raft.is_leader():
                # We may just not have applied their SIGNUP yet — accounts
                # never change once created, so "found" is always right
                # here, but "not found" is the leader's call.
                conn.send(self._not_leader_message(self.raft.get_leader()))
            else:
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

    def _do_signup(self, conn: ClientConnection) -> Optional[str]:
        # Creating a user is a write — only the leader may do it.
        if not self.raft.is_leader():
            conn.send(self._not_leader_message(self.raft.get_leader()))
            return None

        conn.send("Choose username:")
        username = conn.recv()
        if username is None:
            return None

        error = _validate_username(username)
        if error:
            conn.send(f"ERROR: {error}")
            return None
        if self.state_machine.user_exists(username):
            conn.send("ERROR: Username already taken")
            return None

        conn.send("Choose password:")
        password = conn.recv()
        if password is None:
            return None

        if len(password) < MIN_PASSWORD_LEN:
            conn.send(f"ERROR: Password must be at least {MIN_PASSWORD_LEN} characters")
            return None

        # Hash once, here on the leader; every node stores the same hash.
        from django.contrib.auth.hashers import make_password
        ok, created = self._replicate(conn, username, f"SIGNUP {make_password(password)}")
        if not ok:
            return None
        if not created:
            conn.send("ERROR: Username already taken")
            return None
        return username

    # ──────────────────────────────────────────────
    # COMMANDS
    # ──────────────────────────────────────────────

    def _handle_commands(self, conn: ClientConnection, username: str):
        handlers = {
            "SET":    self._cmd_set,
            "GET":    self._cmd_get,
            "HAS":    self._cmd_has,
            "DELETE": self._cmd_delete,
            "INFO":   self._cmd_info,
        }
        while True:
            raw = conn.recv()
            if raw is None:
                break
            if not raw:
                continue
            # split(None, 2) tolerates repeated spaces between the parts
            # while keeping spaces INSIDE the value: SET k hello  world
            parts = raw.split(None, 2)
            command = parts[0].upper()

            if command == "QUIT":
                conn.send("Bye!")
                break
            handler = handlers.get(command)
            if handler is None:
                conn.send("ERROR: Unknown command. Use SET/GET/HAS/DELETE/INFO/QUIT")
                continue
            try:
                handler(conn, username, parts)
            except Exception as e:
                # One bad command must not end the whole session
                logger.exception(f"{command} failed for [{username}]")
                conn.send(f"ERROR: {command} failed: {e}")

    def _cmd_set(self, conn, username, parts):
        if len(parts) != 3:
            conn.send("ERROR: Usage: SET <key> <value>")
            return
        _, key, value = parts
        if len(key) > MAX_KEY_LENGTH:
            conn.send(f"ERROR: Key longer than {MAX_KEY_LENGTH} characters")
            return
        ok, _ = self._replicate(conn, username, f"SET {key} {value}")
        if ok:
            conn.send("OK")

    def _cmd_get(self, conn, username, parts):
        if len(parts) != 2:
            conn.send("ERROR: Usage: GET <key>")
            return
        if not self._may_serve_read(conn):
            return
        value = self.state_machine.get(username, parts[1])
        conn.send(value if value is not None else "NULL")

    def _cmd_has(self, conn, username, parts):
        if len(parts) != 2:
            conn.send("ERROR: Usage: HAS <key>")
            return
        if not self._may_serve_read(conn):
            return
        conn.send("1" if self.state_machine.has(username, parts[1]) else "0")

    def _cmd_delete(self, conn, username, parts):
        if len(parts) != 2:
            conn.send("ERROR: Usage: DELETE <key>")
            return
        ok, existed = self._replicate(conn, username, f"DELETE {parts[1]}")
        if ok:
            conn.send("OK" if existed else "NULL")

    def _cmd_info(self, conn, username, parts):
        status = self.raft.status()
        conn.send(" ".join(f"{k}={v}" for k, v in status.items()))

    # ──────────────────────────────────────────────
    # RAFT GLUE
    # ──────────────────────────────────────────────

    def _replicate(self, conn: ClientConnection, username: str, command: str):
        """
        Push one write through Raft. Returns (True, apply result) once it
        is committed and applied here, or (False, None) after telling the
        client why not.
        """
        try:
            return True, self.raft.propose(command, username, timeout=WRITE_TIMEOUT)
        except NotLeaderError as e:
            conn.send(self._not_leader_message(e.leader_id))
        except ProposalError as e:
            conn.send(f"ERROR: {e}")
        return False, None

    def _may_serve_read(self, conn: ClientConnection) -> bool:
        """Followers only answer reads in stale_reads mode; else redirect."""
        if self.stale_reads or self.raft.is_leader():
            return True
        conn.send(self._not_leader_message(self.raft.get_leader()))
        return False

    def _not_leader_message(self, leader_id: Optional[str]) -> str:
        address = self.client_addresses.get(leader_id) if leader_id else None
        if address:
            return f"ERROR: NOT_LEADER {address}"
        return "ERROR: NO_LEADER election in progress, retry shortly"

    def _on_become_leader(self):
        logger.info(f"Node '{self.node_id}' is now the LEADER, accepting writes")

    def _on_become_follower(self):
        logger.info(f"Node '{self.node_id}' stepped down, redirecting writes to the new leader")


def _validate_username(username: str) -> Optional[str]:
    if len(username) < MIN_USERNAME_LEN:
        return f"Username must be at least {MIN_USERNAME_LEN} characters"
    if len(username) > MAX_USERNAME_LEN:
        return f"Username must be at most {MAX_USERNAME_LEN} characters"
    if any(ch.isspace() for ch in username):
        return "Username cannot contain spaces"
    return None
