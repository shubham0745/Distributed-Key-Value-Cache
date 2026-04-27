"""
server/tcp_server.py  (Week 3 — with MySQL persistence)

Changes from Week 2:
  - _create_user()  now writes to MySQL via db_service
  - _user_exists()  checks MySQL if not found in RAM
  - _verify_password() loads from MySQL on cache miss
  - _cmd_set()      writes to MySQL after writing to RAM
  - _cmd_delete()   deletes from MySQL after RAM
  - _load_from_db() called on startup to restore all data
"""
import socket
import threading
import logging
from typing import Optional

from store.store import Store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s"
)
logger = logging.getLogger(__name__)


class TCPServer:
    """
    Raw TCP server with MySQL-backed persistence.
    Protocol: one command per line, one response per line.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8001,
                 use_db: bool = True):
        self.host = host
        self.port = port
        self.use_db = use_db          # set False in tests to skip MySQL
        self._stores: dict[str, Store] = {}
        self._stores_lock = threading.RLock()
        self._server_socket: Optional[socket.socket] = None
        self._running = False

    # ──────────────────────────────────────────────
    # SERVER LIFECYCLE
    # ──────────────────────────────────────────────

    def start(self):
        if self.use_db:
            self._load_from_db()

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(10)
        self._running = True

        logger.info(f"Server started on {self.host}:{self.port}")
        self._accept_connections()

    def stop(self):
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass

    def _load_from_db(self):
        """
        On startup: read all users + their cache entries from MySQL
        and rebuild the in-memory stores dict.

        Without this, every restart loses all data.
        With this, the server is stateful across restarts.
        """
        try:
            from apps.users.db_service import load_all_users, get_all_entries
            users = load_all_users()
            for u in users:
                store = Store(u["username"], u["password_hash"])
                # Restore all their cache entries
                entries = get_all_entries(u["username"])
                for key, value in entries.items():
                    store.cache.set(key, value)
                with self._stores_lock:
                    self._stores[u["username"]] = store
            logger.info(f"Restored {len(users)} users from MySQL")
        except Exception as e:
            logger.error(f"Failed to load from DB: {e}")

    def _accept_connections(self):
        while self._running:
            try:
                client_socket, address = self._server_socket.accept()
                logger.info(f"New connection from {address}")
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True,
                    name=f"client-{address[1]}"
                )
                client_thread.start()
            except OSError:
                break

    # ──────────────────────────────────────────────
    # CLIENT HANDLING
    # ──────────────────────────────────────────────

    def _handle_client(self, client_socket: socket.socket, address: tuple):
        try:
            self._send(client_socket, "Welcome! Type LOGIN or SIGNUP")
            current_user = self._handle_auth(client_socket)
            if current_user is None:
                return
            self._send(client_socket, f"READY:{current_user}")
            self._handle_commands(client_socket, current_user)
        except (ConnectionResetError, BrokenPipeError):
            logger.info(f"Client {address} disconnected abruptly")
        except Exception as e:
            logger.error(f"Error for {address}: {e}")
        finally:
            client_socket.close()

    # ──────────────────────────────────────────────
    # AUTH
    # ──────────────────────────────────────────────

    def _handle_auth(self, sock: socket.socket) -> Optional[str]:
        while True:
            auth_type = self._recv(sock)
            if auth_type is None:
                return None
            auth_type = auth_type.strip().upper()
            if auth_type == "LOGIN":
                username = self._do_login(sock)
                if username:
                    return username
            elif auth_type == "SIGNUP":
                username = self._do_signup(sock)
                if username:
                    return username
            else:
                self._send(sock, "ERROR: Type LOGIN or SIGNUP")

    def _do_login(self, sock: socket.socket) -> Optional[str]:
        self._send(sock, "Username:")
        username = self._recv(sock)
        if username is None:
            return None
        username = username.strip()

        if not self._user_exists(username):
            self._send(sock, "ERROR: User not found. Try SIGNUP")
            return None

        self._send(sock, "Password:")
        password = self._recv(sock)
        if password is None:
            return None

        if self._verify_password(username, password.strip()):
            return username
        self._send(sock, "ERROR: Wrong password")
        return None

    def _do_signup(self, sock: socket.socket) -> Optional[str]:
        self._send(sock, "Choose username:")
        username = self._recv(sock)
        if username is None:
            return None
        username = username.strip()

        if len(username) < 3:
            self._send(sock, "ERROR: Username must be at least 3 characters")
            return None
        if self._user_exists(username):
            self._send(sock, "ERROR: Username already taken")
            return None

        self._send(sock, "Choose password:")
        password = self._recv(sock)
        if password is None:
            return None
        password = password.strip()

        if len(password) < 4:
            self._send(sock, "ERROR: Password must be at least 4 characters")
            return None

        self._create_user(username, password)
        return username

    # ──────────────────────────────────────────────
    # COMMANDS
    # ──────────────────────────────────────────────

    def _handle_commands(self, sock: socket.socket, username: str):
        while True:
            raw = self._recv(sock)
            if raw is None:
                break
            raw = raw.strip()
            if not raw:
                continue
            parts = raw.split(" ", 2)
            command = parts[0].upper()

            if command == "SET":
                self._cmd_set(sock, username, parts)
            elif command == "GET":
                self._cmd_get(sock, username, parts)
            elif command == "HAS":
                self._cmd_has(sock, username, parts)
            elif command == "DELETE":
                self._cmd_delete(sock, username, parts)
            elif command == "QUIT":
                self._send(sock, "Bye!")
                break
            else:
                self._send(sock, "ERROR: Unknown command. Use SET/GET/HAS/DELETE/QUIT")

    def _cmd_set(self, sock, username, parts):
        if len(parts) != 3:
            self._send(sock, "ERROR: Usage: SET <key> <value>")
            return
        _, key, value = parts
        # Write to RAM first (fast)
        self._get_store(username).cache.set(key, value)
        # Then write to MySQL (persistent)
        if self.use_db:
            try:
                from apps.users.db_service import save_entry
                save_entry(username, key, value)
            except Exception as e:
                logger.error(f"DB write failed for SET [{username}] {key}: {e}")
        self._send(sock, "OK")

    def _cmd_get(self, sock, username, parts):
        if len(parts) != 2:
            self._send(sock, "ERROR: Usage: GET <key>")
            return
        value = self._get_store(username).cache.get(parts[1])
        self._send(sock, value if value is not None else "NULL")

    def _cmd_has(self, sock, username, parts):
        if len(parts) != 2:
            self._send(sock, "ERROR: Usage: HAS <key>")
            return
        result = self._get_store(username).cache.has(parts[1])
        self._send(sock, "1" if result else "0")

    def _cmd_delete(self, sock, username, parts):
        if len(parts) != 2:
            self._send(sock, "ERROR: Usage: DELETE <key>")
            return
        key = parts[1]
        deleted = self._get_store(username).cache.delete(key)
        # Also delete from MySQL
        if self.use_db and deleted:
            try:
                from apps.users.db_service import delete_entry
                delete_entry(username, key)
            except Exception as e:
                logger.error(f"DB delete failed [{username}] {key}: {e}")
        self._send(sock, "OK" if deleted else "NULL")

    # ──────────────────────────────────────────────
    # USER / STORE MANAGEMENT
    # ──────────────────────────────────────────────

    def _user_exists(self, username: str) -> bool:
        """Check RAM first (fast), then MySQL (on cache miss)."""
        with self._stores_lock:
            if username in self._stores:
                return True
        # Not in RAM — check MySQL
        if self.use_db:
            try:
                from apps.users.db_service import user_exists
                return user_exists(username)
            except Exception:
                pass
        return False

    def _verify_password(self, username: str, password: str) -> bool:
        with self._stores_lock:
            store = self._stores.get(username)
        if store:
            return store.verify_password(password)
        # Not in RAM — load from MySQL
        if self.use_db:
            try:
                from apps.users.db_service import get_user
                db_user = get_user(username)
                if db_user:
                    from django.contrib.auth.hashers import check_password
                    return check_password(password, db_user.password_hash)
            except Exception as e:
                logger.error(f"DB verify failed: {e}")
        return False

    def _create_user(self, username: str, password: str):
        from django.contrib.auth.hashers import make_password
        password_hash = make_password(password)
        store = Store(username, password_hash)
        with self._stores_lock:
            self._stores[username] = store
        # Persist to MySQL
        if self.use_db:
            try:
                from apps.users.db_service import save_user
                save_user(username, password_hash)
            except Exception as e:
                logger.error(f"DB save_user failed: {e}")

    def _get_store(self, username: str) -> Store:
        with self._stores_lock:
            return self._stores[username]

    # ──────────────────────────────────────────────
    # SOCKET HELPERS
    # ──────────────────────────────────────────────

    def _send(self, sock: socket.socket, message: str):
        try:
            sock.sendall((message + "\n").encode("utf-8"))
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _recv(self, sock: socket.socket, buffer_size: int = 4096) -> Optional[str]:
        """Read exactly one line — no batching issues."""
        try:
            data = b""
            while not data.endswith(b"\n"):
                chunk = sock.recv(1)
                if not chunk:
                    return None
                data += chunk
            return data.decode("utf-8").strip()
        except (ConnectionResetError, OSError):
            return None