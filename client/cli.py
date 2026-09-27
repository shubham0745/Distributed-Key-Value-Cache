"""
client/cli.py — command-line client for the distributed cache

    python client/cli.py                                   # interactive
    python client/cli.py --nodes 127.0.0.1:8001,127.0.0.1:8002
    python client/cli.py --user shubham --password secret GET city   # one-shot
    python client/cli.py --admin MEMBERS                   # admin session

Any node accepts any command, so plain telnet works too. This client adds:
  - failover: when the node it uses dies, it moves to another one
  - retries while an election is running (NO_LEADER) — safely: every
    command carries a request id "@client_id:seq", so a retried write is
    applied only once even if the first attempt actually went through
  - redirects: admin membership changes must run on the leader
  - TLS, when cluster.json has a "tls" section (or with --ca)
By default it knows 127.0.0.1:8001 plus every node in ../cluster.json.

Standard library only, so it runs anywhere Python does. CacheClient can
also be imported and used from your own code:

    with CacheClient(["127.0.0.1:8001"]) as c:
        c.login("shubham", "secret")
        c.set("city", "gurugram")
        print(c.get("city"))
"""
import argparse
import getpass
import json
import os
import socket
import ssl
import sys
import time
import uuid
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent

REDIRECT_PREFIX = "ERROR: NOT_LEADER "
RETRYABLE_PREFIXES = (
    "ERROR: NO_LEADER",                     # election in progress
    "ERROR: timed out",                     # no majority / leader right now
    "ERROR: write was replaced",            # leader changed mid-write
)
FAILED_NODE_COOLDOWN = 5.0                  # try other nodes first for this long
BANNER_TIMEOUT = 3.0                        # a node greets us at once; silence = wrong protocol


class ClientError(Exception):
    """The request could not be completed."""


class AuthError(ClientError):
    """LOGIN / SIGNUP / ADMIN was refused (wrong password, name taken, ...)."""


class ProtocolMismatchError(ClientError):
    """The node speaks TLS and we don't (or the other way round)."""


class _NotReplicatedYet(Exception):
    """A node doesn't know our user yet — it is still catching up."""


class CacheClient:

    def __init__(self, nodes, timeout: float = 10.0, connect_timeout: float = 1.0,
                 retry_for: float = 10.0, ssl_context: Optional[ssl.SSLContext] = None):
        self.nodes = [nodes] if isinstance(nodes, str) else list(nodes)
        if not self.nodes:
            raise ValueError("at least one node address is required")
        self.timeout = timeout                  # waiting for a reply
        self.connect_timeout = connect_timeout  # opening a connection
        self.retry_for = retry_for              # total time spent retrying one request
        self.ssl_context = ssl_context
        self.address: Optional[str] = None      # node we are connected to
        self.username: Optional[str] = None
        self._mode: Optional[str] = None        # "LOGIN" or "ADMIN" once authenticated
        self._secret: Optional[str] = None      # password or admin token
        self._sock = None
        self._reader = None
        self._preferred: Optional[str] = None   # where the last redirect pointed
        self._failed_at: dict[str, float] = {}
        # Request ids: a random client id + a counter, one per logical request
        self._client_id = uuid.uuid4().hex[:16]
        self._seq = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ──────────────────────────────────────────────
    # AUTH
    # ──────────────────────────────────────────────

    def login(self, username: str, password: str):
        self._authenticate_as("LOGIN", username, password)

    def signup(self, username: str, password: str):
        self._authenticate_as("SIGNUP", username, password)

    def admin_login(self, token: str):
        self._authenticate_as("ADMIN", "admin", token)

    def _authenticate_as(self, mode: str, username: str, secret: str):
        self._disconnect()                      # always start on a fresh connection
        self.username = self._secret = self._mode = None
        tag = self._next_tag() if mode == "SIGNUP" else None
        reply = self._with_retries(lambda: self._auth_exchange(mode, username, secret, tag),
                                   logged_in=False)
        if not reply.startswith("READY"):
            self._disconnect()
            raise AuthError(reply.removeprefix("ERROR: "))
        self.username, self._secret = username, secret
        self._mode = "ADMIN" if mode == "ADMIN" else "LOGIN"

    # ──────────────────────────────────────────────
    # COMMANDS
    # ──────────────────────────────────────────────

    def execute(self, command: str) -> str:
        """Send one raw command (e.g. "SET k v") and return the reply line."""
        if self._mode is None:
            raise ClientError("log in first (login(), signup() or admin_login())")
        if "\n" in command or "\r" in command:
            raise ClientError("commands cannot contain line breaks")
        line = f"@{self._next_tag()} {command}"     # same id on every retry
        return self._with_retries(lambda: self._request(line), logged_in=True)

    def set(self, key: str, value: str) -> None:
        self._expect_ok(self._checked(f"SET {key} {value}"))

    def setex(self, key: str, seconds: int, value: str) -> None:
        self._expect_ok(self._checked(f"SETEX {key} {seconds} {value}"))

    def get(self, key: str) -> Optional[str]:
        reply = self._checked(f"GET {key}")
        return None if reply == "NULL" else reply

    def has(self, key: str) -> bool:
        return self._checked(f"HAS {key}") == "1"

    def delete(self, key: str) -> bool:
        return self._checked(f"DELETE {key}") == "OK"

    def expire(self, key: str, seconds: int) -> bool:
        return self._checked(f"EXPIRE {key} {seconds}") == "1"

    def persist(self, key: str) -> bool:
        return self._checked(f"PERSIST {key}") == "1"

    def ttl(self, key: str) -> int:
        return int(self._checked(f"TTL {key}"))

    def keys(self, pattern: Optional[str] = None) -> list[str]:
        reply = self._checked("KEYS" + (f" {pattern}" if pattern else ""))
        return reply.split()[1:]

    def info(self) -> dict:
        reply = self._checked("INFO")
        return dict(part.split("=", 1) for part in reply.split() if "=" in part)

    def members(self) -> dict:
        """{"leader": id, node_id: "client_addr/raft_addr", ...}"""
        reply = self._checked("MEMBERS")
        return dict(part.split("=", 1) for part in reply.split() if "=" in part)

    # admin session only
    def add_node(self, node_id: str, client_address: str, raft_address: str) -> None:
        self._expect_ok(self._checked(f"ADDNODE {node_id} {client_address} {raft_address}"))

    def remove_node(self, node_id: str) -> None:
        self._expect_ok(self._checked(f"REMOVENODE {node_id}"))

    def import_user(self, username: str, password_hash: str) -> bool:
        """True if created, False if the user already existed."""
        return self._checked(f"IMPORTUSER {username} {password_hash}") == "OK"

    def import_set(self, username: str, key: str, value: str, expire_at_ms: int = 0) -> str:
        """"OK", or "EXPIRED" if the key's expiry has already passed."""
        return self._checked(f"IMPORTSET {username} {key} {expire_at_ms} {value}")

    def close(self):
        if self._sock is not None:
            try:
                self._sock.sendall(b"QUIT\n")
            except OSError:
                pass
        self._disconnect()

    def _checked(self, command: str) -> str:
        reply = self.execute(command)
        if reply.startswith("ERROR"):
            raise ClientError(reply.removeprefix("ERROR: "))
        return reply

    @staticmethod
    def _expect_ok(reply: str):
        if reply != "OK":
            raise ClientError(f"unexpected reply: {reply}")

    def _next_tag(self) -> str:
        self._seq += 1
        return f"{self._client_id}:{self._seq}"

    # ──────────────────────────────────────────────
    # REDIRECTS, FAILOVER, RETRIES
    # ──────────────────────────────────────────────

    def _with_retries(self, attempt: Callable[[], str], logged_in: bool) -> str:
        deadline = time.monotonic() + self.retry_for
        problem = "no node reachable"
        first = True
        while True:
            if not first and time.monotonic() >= deadline:
                raise ClientError(f"gave up after {self.retry_for:.0f}s: {problem}")
            first = False

            try:
                self._ensure_connected(logged_in)
                reply = attempt()
            except _NotReplicatedYet as e:
                problem = str(e)
            except OSError as e:
                problem = f"{self.address or 'cluster'} unreachable ({e})"
                self._mark_failed(self.address)
                self._disconnect()
            else:
                if reply.startswith(REDIRECT_PREFIX):
                    leader = reply[len(REDIRECT_PREFIX):].strip()
                    if leader not in self.nodes:
                        self.nodes.append(leader)
                    self._preferred = leader
                    self._disconnect()
                    problem = reply
                    continue                    # go to the leader straight away
                if not reply.startswith(RETRYABLE_PREFIXES):
                    return reply
                problem = reply
            time.sleep(0.2)

    def _ensure_connected(self, logged_in: bool):
        if self._sock is not None:
            return
        self._connect_any()
        if not logged_in:
            return
        reply = self._auth_exchange(self._mode, self.username, self._secret)
        if reply.startswith("READY"):
            return
        self._disconnect()
        if "not found" in reply.lower() or reply.startswith(RETRYABLE_PREFIXES):
            # We signed up a moment ago and this node hasn't heard yet,
            # or it is mid-election. Either way: try again shortly.
            raise _NotReplicatedYet(reply)
        raise AuthError(reply.removeprefix("ERROR: "))

    def _connect_any(self):
        """Preferred node first, then healthy nodes, then recently failed ones."""
        now = time.monotonic()
        others = [a for a in self.nodes if a != self._preferred]
        healthy = [a for a in others if now - self._failed_at.get(a, -1e9) > FAILED_NODE_COOLDOWN]
        failing = [a for a in others if a not in healthy]
        candidates = ([self._preferred] if self._preferred else []) + healthy + failing

        error = None
        for address in candidates:
            try:
                self._connect(address)
                return
            except OSError as e:
                error = e
                self._mark_failed(address)
                if address == self._preferred:
                    self._preferred = None
        raise ConnectionError(f"no node reachable ({error})")

    def _connect(self, address: str):
        host, port = address.rsplit(":", 1)
        sock = socket.create_connection((host, int(port)), timeout=self.connect_timeout)
        try:
            if self.ssl_context:
                sock = self.ssl_context.wrap_socket(sock, server_hostname=host)
            sock.settimeout(min(self.timeout, BANNER_TIMEOUT))
        except (OSError, ssl.SSLError):
            sock.close()
            raise
        self._sock, self._reader, self.address = sock, sock.makefile("rb"), address
        try:
            self._read_line()                   # "Welcome! Type LOGIN or SIGNUP"
        except socket.timeout:
            self._disconnect()
            # A TLS server waits silently for a handshake a plaintext client
            # never sends. Retrying can't fix that, so fail right away.
            raise ProtocolMismatchError(f"{address} sent no welcome message - if the cluster "
                                        f"uses TLS, pass --ca (or add tls to cluster.json)") from None
        except OSError:
            self._disconnect()
            raise
        sock.settimeout(self.timeout)

    def _disconnect(self):
        for resource in (self._reader, self._sock):
            if resource is not None:
                try:
                    resource.close()
                except OSError:
                    pass
        self._sock = self._reader = None
        self.address = None

    def _mark_failed(self, address: Optional[str]):
        if address:
            self._failed_at[address] = time.monotonic()
            if address == self._preferred:
                self._preferred = None

    # ──────────────────────────────────────────────
    # WIRE PROTOCOL
    # ──────────────────────────────────────────────

    def _auth_exchange(self, mode: str, username: str, secret: str,
                       tag: Optional[str] = None) -> str:
        """LOGIN/SIGNUP → username → password, or ADMIN → token. Returns READY:... or the first ERROR."""
        reply = self._request(f"@{tag} {mode}" if tag else mode)
        if reply.startswith("ERROR"):
            return reply
        if mode != "ADMIN":
            reply = self._request(username)
            if reply.startswith("ERROR"):
                return reply
        return self._request(secret)

    def _request(self, line: str) -> str:
        self._sock.sendall((line + "\n").encode("utf-8"))
        return self._read_line()

    def _read_line(self) -> str:
        line = self._reader.readline()
        if not line:
            raise ConnectionError("server closed the connection")
        return line.decode("utf-8", errors="replace").rstrip("\r\n")


# ──────────────────────────────────────────────
# COMMAND LINE
# ──────────────────────────────────────────────

HELP = """Commands:
  SET <key> <value>          store a value (the value may contain spaces)
  SETEX <key> <secs> <value> store a value that expires after <secs>
  GET <key>                  read a value (NULL if missing)
  HAS <key>                  1 if the key exists, else 0
  DELETE <key>               remove a key
  EXPIRE <key> <secs>        make an existing key expire
  PERSIST <key>              remove a key's expiry
  TTL <key>                  seconds left (-1 no expiry, -2 no such key)
  KEYS [pattern]             list your keys (glob pattern, e.g. user:*)
  INFO                       Raft status of the node you're connected to
  MEMBERS                    the cluster's nodes and its leader
  HELP                       show this text
  EXIT                       quit"""

ADMIN_HELP = """Admin commands:
  MEMBERS                                  the cluster's nodes and its leader
  ADDNODE <id> <client host:port> <raft host:port>
                                           add a node (it must be running)
  REMOVENODE <id>                          remove a node from the cluster
  IMPORTUSER <username> <password_hash>    (used by scripts/import_data.py)
  IMPORTSET <username> <key> <expire_at_ms|0> <value>
  INFO                                     Raft status of this node
  HELP / EXIT"""

# Same rules the server enforces (server/tcp_server.py) — checked here too
# so a typo is caught before anything is sent.
MIN_USERNAME_LEN = 3
MIN_PASSWORD_LEN = 4


def load_cluster_json(path) -> Optional[dict]:
    path = Path(path)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    data["_dir"] = str(path.resolve().parent)
    return data


def nodes_from_cluster(data: dict) -> list[str]:
    return [f"{n.get('host', '127.0.0.1')}:{n['client_port']}" for n in data["nodes"]]


def resolve_nodes(args, cluster: Optional[dict]) -> list[str]:
    if args.nodes:
        return [a.strip() for a in args.nodes.split(",") if a.strip()]
    if args.cluster and cluster:
        return nodes_from_cluster(cluster)
    nodes = ["127.0.0.1:8001"]
    if cluster:
        nodes += [a for a in nodes_from_cluster(cluster) if a not in nodes]
    return nodes


def resolve_ssl(args, cluster: Optional[dict]) -> Optional[ssl.SSLContext]:
    ca = args.ca
    if not ca and cluster and cluster.get("tls"):
        ca = os.path.join(cluster["_dir"], cluster["tls"]["ca"])
    if not ca:
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(ca)
    return ctx


def ask_mode() -> Optional[str]:
    """Returns "login", "signup", or None to quit. Re-asks on anything else."""
    while True:
        choice = input("LOGIN or SIGNUP? [login] ").strip().lower() or "login"
        if choice in ("l", "login"):
            return "login"
        if choice in ("s", "signup"):
            return "signup"
        if choice in ("q", "quit", "exit"):
            return None
        print("Please type LOGIN or SIGNUP (or QUIT).")


def ask_username(mode: str) -> str:
    while True:
        username = input("Username: ").strip()
        if not username:
            print("Username cannot be empty")
        elif mode == "signup" and len(username) < MIN_USERNAME_LEN:
            print(f"Username must be at least {MIN_USERNAME_LEN} characters")
        elif mode == "signup" and any(ch.isspace() for ch in username):
            print("Username cannot contain spaces")
        else:
            return username


def ask_password(mode: str) -> str:
    """Hidden input. For SIGNUP: enforce the length rule and ask twice."""
    while True:
        password = getpass.getpass("Password (hidden as you type): ")
        if mode == "login":
            if password.strip():
                return password
            print("Password cannot be empty")
            continue
        if len(password.strip()) < MIN_PASSWORD_LEN:
            print(f"Password must be at least {MIN_PASSWORD_LEN} characters")
            continue
        if getpass.getpass("Confirm password: ") != password:
            print("Passwords don't match, try again")
            continue
        return password


def authenticate(client: CacheClient, args, cluster: Optional[dict] = None) -> bool:
    """Log in (or sign up / open an admin session), prompting only for what's missing."""
    if args.admin:
        token = (args.admin_token or os.environ.get("CACHE_ADMIN_TOKEN") or
                 (cluster or {}).get("admin_token"))
        try:
            if not token:
                token = getpass.getpass("Admin token (hidden as you type): ")
            client.admin_login(token)
            print(f"Admin session on {client.address}")
            return True
        except (EOFError, KeyboardInterrupt):
            print()
            return False
        except ClientError as e:
            print(f"Error: {e}")
            return False

    if args.user and args.password:
        mode = "signup" if args.signup else "login"
        try:
            (client.signup if mode == "signup" else client.login)(args.user, args.password)
            print(f"Logged in as {args.user} on {client.address}")
            return True
        except ClientError as e:
            print(f"Error: {e}")
            return False

    try:
        mode = "signup" if args.signup else ask_mode()
        username = args.user
        while mode:
            username = username or ask_username(mode)
            password = ask_password(mode)
            try:
                (client.signup if mode == "signup" else client.login)(username, password)
                print(f"Logged in as {username} on {client.address}")
                return True
            except AuthError as e:
                print(f"Error: {e}")
                if "wrong password" in str(e).lower():
                    continue                    # same account — just re-ask the password
                if "not found" in str(e).lower():
                    print("Tip: choose SIGNUP to create this account.")
                username = None
                mode = ask_mode()
        return False
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    except ClientError as e:
        print(f"Error: {e}")
        return False


def repl(client: CacheClient, admin: bool = False):
    print("Type HELP for commands, EXIT to quit.")
    while True:
        try:
            line = input(f"{client.username}@{client.address or 'reconnecting'}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not line:
            continue
        word = line.split()[0].upper()
        if word in ("EXIT", "QUIT"):
            return
        if word == "HELP":
            print(ADMIN_HELP if admin else HELP)
            continue
        before = client.address
        try:
            reply = client.execute(line)
        except ClientError as e:
            print(f"(error) {e}")
            continue
        if client.address != before:
            print(f"(now connected to {client.address})")
        print(reply)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Client for the distributed key-value cache")
    parser.add_argument("--nodes", help="comma-separated host:port list")
    parser.add_argument("--cluster", help="read node addresses (and TLS settings) from this cluster.json")
    parser.add_argument("--user", help="username (prompted if omitted)")
    parser.add_argument("--password", help="password (prompted if omitted)")
    parser.add_argument("--signup", action="store_true", help="create the account instead of logging in")
    parser.add_argument("--admin", action="store_true", help="open an admin session")
    parser.add_argument("--admin-token", help="admin token (default: $CACHE_ADMIN_TOKEN or cluster.json)")
    parser.add_argument("--ca", help="CA certificate to trust for TLS (default: from cluster.json)")
    parser.add_argument("command", nargs=argparse.REMAINDER,
                        help="run one command and exit, e.g. SET city gurugram")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    cluster = load_cluster_json(args.cluster or ROOT / "cluster.json")
    client = CacheClient(resolve_nodes(args, cluster), ssl_context=resolve_ssl(args, cluster))
    try:
        if not authenticate(client, args, cluster):
            return 1
        if args.command:
            try:
                print(client.execute(" ".join(args.command)))
                return 0
            except ClientError as e:
                print(f"Error: {e}", file=sys.stderr)
                return 1
        repl(client, admin=args.admin)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
