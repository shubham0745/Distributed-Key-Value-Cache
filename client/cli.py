"""
client/cli.py — command-line client for the distributed cache (Week 5)

    python client/cli.py                                   # interactive
    python client/cli.py --nodes 127.0.0.1:8001,127.0.0.1:8002
    python client/cli.py --user shubham --password secret GET city   # one-shot

Why not just telnet? In a cluster only the leader accepts writes, and
telnet leaves "ERROR: NOT_LEADER 127.0.0.1:8002" for you to deal with.
This client:
  - follows NOT_LEADER redirects and logs in again on the leader
  - fails over to another node when the one it uses dies
  - retries while an election is running (NO_LEADER)
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
import socket
import sys
import time
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent

REDIRECT_PREFIX = "ERROR: NOT_LEADER "
RETRYABLE_PREFIXES = (
    "ERROR: NO_LEADER",                     # election in progress
    "ERROR: timed out waiting",             # no majority right now
    "ERROR: write was replaced",            # leader changed mid-write
)
FAILED_NODE_COOLDOWN = 5.0                  # try other nodes first for this long


class ClientError(Exception):
    """The request could not be completed."""


class AuthError(ClientError):
    """LOGIN / SIGNUP was refused (wrong password, name taken, ...)."""


class _NotReplicatedYet(Exception):
    """A node doesn't know our user yet — it is still catching up."""


class CacheClient:

    def __init__(self, nodes, timeout: float = 10.0, connect_timeout: float = 1.0,
                 retry_for: float = 10.0):
        self.nodes = [nodes] if isinstance(nodes, str) else list(nodes)
        if not self.nodes:
            raise ValueError("at least one node address is required")
        self.timeout = timeout                  # waiting for a reply
        self.connect_timeout = connect_timeout  # opening a connection
        self.retry_for = retry_for              # total time spent retrying one request
        self.address: Optional[str] = None      # node we are connected to
        self.username: Optional[str] = None
        self._password: Optional[str] = None
        self._sock: Optional[socket.socket] = None
        self._reader = None
        self._preferred: Optional[str] = None   # where the last redirect pointed
        self._failed_at: dict[str, float] = {}

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

    def _authenticate_as(self, mode: str, username: str, password: str):
        self._disconnect()                      # always start on a fresh connection
        self.username = self._password = None
        reply = self._with_retries(lambda: self._auth_exchange(mode, username, password),
                                   logged_in=False)
        if not reply.startswith("READY"):
            self._disconnect()
            raise AuthError(reply.removeprefix("ERROR: "))
        self.username, self._password = username, password

    # ──────────────────────────────────────────────
    # COMMANDS
    # ──────────────────────────────────────────────

    def execute(self, command: str) -> str:
        """Send one raw command (e.g. "SET k v") and return the reply line."""
        if self.username is None:
            raise ClientError("log in first (login() or signup())")
        if "\n" in command or "\r" in command:
            raise ClientError("commands cannot contain line breaks")
        return self._with_retries(lambda: self._request(command), logged_in=True)

    def set(self, key: str, value: str) -> None:
        reply = self._checked(f"SET {key} {value}")
        if reply != "OK":
            raise ClientError(f"unexpected reply: {reply}")

    def get(self, key: str) -> Optional[str]:
        reply = self._checked(f"GET {key}")
        return None if reply == "NULL" else reply

    def has(self, key: str) -> bool:
        return self._checked(f"HAS {key}") == "1"

    def delete(self, key: str) -> bool:
        return self._checked(f"DELETE {key}") == "OK"

    def info(self) -> dict:
        reply = self._checked("INFO")
        return dict(part.split("=", 1) for part in reply.split() if "=" in part)

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
        reply = self._auth_exchange("LOGIN", self.username, self._password)
        if reply.startswith("READY"):
            return
        self._disconnect()
        if "not found" in reply.lower():
            # We signed up on the leader a moment ago; this node hasn't
            # applied that entry yet. It will within a heartbeat or two.
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
        sock.settimeout(self.timeout)
        self._sock, self._reader, self.address = sock, sock.makefile("rb"), address
        try:
            self._read_line()                   # "Welcome! Type LOGIN or SIGNUP"
        except OSError:
            self._disconnect()
            raise

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

    def _auth_exchange(self, mode: str, username: str, password: str) -> str:
        """LOGIN/SIGNUP → username → password. Returns READY:... or the first ERROR."""
        reply = self._request(mode)
        if reply.startswith("ERROR"):
            return reply
        reply = self._request(username)
        if reply.startswith("ERROR"):
            return reply
        return self._request(password)

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
  SET <key> <value>   store a value (the value may contain spaces)
  GET <key>           read a value (NULL if missing)
  HAS <key>           1 if the key exists, else 0
  DELETE <key>        remove a key
  INFO                Raft status of the node you're connected to
  HELP                show this text
  EXIT                quit"""


def nodes_from_cluster(path) -> list[str]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return [f"{n.get('host', '127.0.0.1')}:{n['client_port']}" for n in data["nodes"]]


def resolve_nodes(args) -> list[str]:
    if args.nodes:
        return [a.strip() for a in args.nodes.split(",") if a.strip()]
    if args.cluster:
        return nodes_from_cluster(args.cluster)
    nodes = ["127.0.0.1:8001"]
    default_cluster = ROOT / "cluster.json"
    if default_cluster.exists():
        nodes += [a for a in nodes_from_cluster(default_cluster) if a not in nodes]
    return nodes


# Same rules the server enforces (server/tcp_server.py) — checked here too
# so a typo is caught before anything is sent.
MIN_USERNAME_LEN = 3
MIN_PASSWORD_LEN = 4


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


def authenticate(client: CacheClient, args) -> bool:
    """Log in (or sign up) from flags, prompting only for what's missing."""
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


def repl(client: CacheClient):
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
            print(HELP)
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
    parser.add_argument("--cluster", help="read node addresses from this cluster.json")
    parser.add_argument("--user", help="username (prompted if omitted)")
    parser.add_argument("--password", help="password (prompted if omitted)")
    parser.add_argument("--signup", action="store_true", help="create the account instead of logging in")
    parser.add_argument("command", nargs=argparse.REMAINDER,
                        help="run one command and exit, e.g. SET city gurugram")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    client = CacheClient(resolve_nodes(args))
    try:
        if not authenticate(client, args):
            return 1
        if args.command:
            try:
                print(client.execute(" ".join(args.command)))
                return 0
            except ClientError as e:
                print(f"Error: {e}", file=sys.stderr)
                return 1
        repl(client)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
