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
    Raw TCP server — no HTTP, no Django views, pure sockets.

    This mirrors tcp_server.go from the original project.

    WHY RAW TCP instead of HTTP/REST?
    Redis, Memcached, PostgreSQL — all use raw TCP with their own
    text protocol. It's faster (no HTTP overhead), more educational,
    and makes this project stand out vs a typical Django REST API.

    HOW IT WORKS:
    1. Server binds to a port and listens
    2. For every new client → spawn a NEW thread to handle it
    3. That thread handles auth (LOGIN/SIGNUP) first
    4. Once authenticated → handle commands (SET/GET/HAS/DELETE)
    5. Client disconnects → thread dies

    PROTOCOL (what client types):
        LOGIN              → server asks for username + password
        SIGNUP             → server asks for username + password → creates user
        SET key value      → stores key=value in user's cache
        GET key            → retrieves value
        HAS key            → returns 1 or 0
        DELETE key         → removes key
        QUIT               → closes connection
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8001):
        self.host = host
        self.port = port
        self._stores: dict[str, Store] = {}   # username → Store
        self._stores_lock = threading.RLock()  # protect the stores dict
        self._server_socket: Optional[socket.socket] = None
        self._running = False

    # ──────────────────────────────────────────────
    # SERVER LIFECYCLE
    # ──────────────────────────────────────────────

    def start(self):
        """
        Bind to port, start accepting connections.
        Blocks forever (run in main thread or a dedicated thread).
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # SO_REUSEADDR: lets us restart server immediately without
        # "Address already in use" error — critical during development
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(10)  # queue up to 10 pending connections
        self._running = True

        logger.info(f"Server started on {self.host}:{self.port}")
        logger.info("Waiting for connections...")

        self._accept_connections()

    def stop(self):
        """Gracefully shut down the server."""
        self._running = False
        if self._server_socket:
            self._server_socket.close()
        logger.info("Server stopped.")

    def _accept_connections(self):
        """
        Main loop: accept a client → hand off to a new thread.
        This is NON-BLOCKING for the server — it immediately goes
        back to accepting the next client while the thread handles
        the current one.
        """
        while self._running:
            try:
                client_socket, address = self._server_socket.accept()
                logger.info(f"New connection from {address}")

                # Each client gets its own thread
                # daemon=True means thread dies when main program exits
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True,
                    name=f"client-{address[1]}"
                )
                client_thread.start()

            except OSError:
                # Server socket was closed (during shutdown)
                break

    # ──────────────────────────────────────────────
    # CLIENT HANDLING
    # ──────────────────────────────────────────────

    def _handle_client(self, client_socket: socket.socket, address: tuple):
        """
        Runs in its own thread for each connected client.
        Phase 1: Authentication (LOGIN or SIGNUP)
        Phase 2: Command handling (SET/GET/HAS/DELETE)
        """
        try:
            self._send(client_socket, "Welcome! Type LOGIN or SIGNUP")

            # Phase 1: Auth
            current_user = self._handle_auth(client_socket)
            if current_user is None:
                # Auth failed or client disconnected
                return

            self._send(client_socket, f"READY:{current_user}")
            logger.info(f"{address} authenticated as '{current_user}'")

            # Phase 2: Commands
            self._handle_commands(client_socket, current_user)

        except (ConnectionResetError, BrokenPipeError):
            logger.info(f"Client {address} disconnected abruptly")
        except Exception as e:
            logger.error(f"Unexpected error for {address}: {e}")
        finally:
            client_socket.close()
            logger.info(f"Connection closed: {address}")

    # ──────────────────────────────────────────────
    # AUTH PHASE
    # ──────────────────────────────────────────────

    def _handle_auth(self, sock: socket.socket) -> Optional[str]:
        """
        Keeps prompting until user successfully LOGINs or SIGNUPs.
        Returns the authenticated username, or None on disconnect.
        """
        while True:
            auth_type = self._recv(sock)
            if auth_type is None:
                return None  # Client disconnected

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
        """
        LOGIN flow:
        1. Ask for username
        2. Check if user exists
        3. Ask for password
        4. Verify hash
        """
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
        password = password.strip()

        if self._verify_password(username, password):
            return username
        else:
            self._send(sock, "ERROR: Wrong password")
            return None

    def _do_signup(self, sock: socket.socket) -> Optional[str]:
        """
        SIGNUP flow:
        1. Ask for username
        2. Check username not already taken
        3. Ask for password
        4. Hash password, create Store, save to MySQL
        """
        self._send(sock, "Choose username:")
        username = self._recv(sock)
        if username is None:
            return None
        username = username.strip()

        if not username or len(username) < 3:
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

        # Create the user
        self._create_user(username, password)
        # self._send(sock, f"Account created for '{username}'")
        return username

    # ──────────────────────────────────────────────
    # COMMAND PHASE
    # ──────────────────────────────────────────────

    def _handle_commands(self, sock: socket.socket, username: str):
        """
        Main command loop after authentication.
        Reads commands line by line, dispatches to the right handler.
        """
        while True:
            raw = self._recv(sock)
            if raw is None:
                break  # Client disconnected

            raw = raw.strip()
            if not raw:
                continue

            parts = raw.split(" ", 2)  # max 3 parts: CMD key value
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

    def _cmd_set(self, sock: socket.socket, username: str, parts: list):
        # SET key value
        if len(parts) != 3:
            self._send(sock, "ERROR: Usage: SET <key> <value>")
            return
        _, key, value = parts
        self._get_store(username).cache.set(key, value)
        self._send(sock, "OK")
        logger.info(f"[{username}] SET {key}")

    def _cmd_get(self, sock: socket.socket, username: str, parts: list):
        # GET key
        if len(parts) != 2:
            self._send(sock, "ERROR: Usage: GET <key>")
            return
        key = parts[1]
        value = self._get_store(username).cache.get(key)
        if value is None:
            self._send(sock, "NULL")
        else:
            self._send(sock, value)

    def _cmd_has(self, sock: socket.socket, username: str, parts: list):
        # HAS key
        if len(parts) != 2:
            self._send(sock, "ERROR: Usage: HAS <key>")
            return
        key = parts[1]
        result = self._get_store(username).cache.has(key)
        self._send(sock, "1" if result else "0")

    def _cmd_delete(self, sock: socket.socket, username: str, parts: list):
        # DELETE key
        if len(parts) != 2:
            self._send(sock, "ERROR: Usage: DELETE <key>")
            return
        key = parts[1]
        deleted = self._get_store(username).cache.delete(key)
        self._send(sock, "OK" if deleted else "NULL")
        logger.info(f"[{username}] DELETE {key}")

    # ──────────────────────────────────────────────
    # USER / STORE MANAGEMENT
    # ──────────────────────────────────────────────

    def _user_exists(self, username: str) -> bool:
        """Check in-memory stores dict first, then MySQL (Week 3)."""
        with self._stores_lock:
            return username in self._stores

    def _verify_password(self, username: str, password: str) -> bool:
        with self._stores_lock:
            store = self._stores.get(username)
            if store is None:
                return False
            return store.verify_password(password)

    def _create_user(self, username: str, password: str):
        """
        Hash the password and create a Store for the new user.
        In Week 3 we'll also persist this to MySQL.
        """
        from django.contrib.auth.hashers import make_password
        password_hash = make_password(password)

        with self._stores_lock:
            self._stores[username] = Store(username, password_hash)

        logger.info(f"New user created: '{username}'")

    def _get_store(self, username: str) -> Store:
        with self._stores_lock:
            return self._stores[username]

    # ──────────────────────────────────────────────
    # SOCKET HELPERS
    # ──────────────────────────────────────────────

    def _send(self, sock: socket.socket, message: str):
        """
        Send a message to the client.
        We append \n so the client knows where the message ends.
        All messages in our protocol are newline-terminated.
        """
        try:
            sock.sendall((message + "\n").encode("utf-8"))
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _recv(self, sock: socket.socket, buffer_size: int = 4096) -> Optional[str]:
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

    def __repr__(self) -> str:
        return f"TCPServer({self.host}:{self.port}, users={len(self._stores)})"