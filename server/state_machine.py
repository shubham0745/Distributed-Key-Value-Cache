"""
server/state_machine.py  (Week 5, extended in Week 7)

The "state machine" in Raft terms: the data every node must agree on —
which users exist (with password hashes), each user's key/values and
their expiry times, and the client session table.

Raft guarantees every node calls apply() with the SAME entries in the
SAME order. apply() is deterministic, so every node ends up with the
same data. Reads never go through the log; the server calls
raft.read_index() first when it needs them to be current.

DETERMINISTIC EXPIRY
apply() never looks at the local clock — clocks differ between nodes and
replaying the log later would give different answers. Instead the node
that receives a command stamps the time INTO it:
    SETEX key <expire_at_ms> value
    EXPIREAT key <expire_at_ms> <now_ms>
    PERSIST key <now_ms>
    DELETE key <now_ms>
    SWEEP <now_ms>          (the leader deletes expired keys everywhere)
Reads hide keys that have expired by the local clock right away; SWEEP
entries then remove them for good, identically on every node.

EXACTLY-ONCE WRITES
Each entry may carry a request_id "client_id:seq". The session table
remembers each client's last seq and its result: a retried request
(same id) returns the stored result instead of running twice.

Storage layers when use_db=True:
    RAM   — one LRUCache per user (fast, holds at most `capacity` keys)
    MySQL — cache_users / cache_entries / client_sessions (durable)
A RAM miss falls back to MySQL and warms the cache again, so an LRU
eviction never loses data. With use_db=False RAM is the only copy and
an eviction really forgets the key — a pure cache.
"""
import fnmatch
import json
import logging
import threading
import time
from typing import Any, Optional

from raft.types import LogEntry
from store.store import Store

logger = logging.getLogger(__name__)

MAX_SESSIONS = 10_000        # oldest client sessions are forgotten beyond this
_MISS = object()


def now_ms() -> int:
    return int(time.time() * 1000)


class StateLoadError(RuntimeError):
    """The database could not be read at startup."""


class CacheStateMachine:

    def __init__(self, use_db: bool = True, capacity: int = 1000):
        self.use_db = use_db
        self.capacity = capacity
        self.stores: dict[str, Store] = {}
        self.sessions: dict[str, tuple[int, Any, int]] = {}   # client → (seq, result, log index)
        self._lock = threading.RLock()     # guards the stores and sessions dicts

    # ──────────────────────────────────────────────
    # STARTUP
    # ──────────────────────────────────────────────

    def load_from_db(self):
        """
        Rebuild the in-memory stores from MySQL: every user, plus each
        user's most recent `capacity` keys (older ones load on demand),
        and the client session table.

        Raises StateLoadError instead of starting empty — an empty RAM
        view over a full database would reject valid logins and let
        SIGNUP re-create existing users.
        """
        try:
            from apps.users.db_service import (load_all_users, get_all_entries,
                                               get_expiries, load_sessions)
            users = load_all_users()
            stores = {}
            for u in users:
                store = Store(u["username"], u["password_hash"], capacity=self.capacity)
                # Restore their cache entries
                entries = get_all_entries(u["username"], limit=self.capacity)
                for key, value in entries.items():
                    store.cache.set(key, value)
                store.expiry = {k: exp for k, exp in get_expiries(u["username"]).items()
                                if k in entries}
                stores[u["username"]] = store
            sessions = {cid: (seq, json.loads(result), index)
                        for cid, seq, result, index in load_sessions()}
        except Exception as e:
            raise StateLoadError(f"could not load data from the database: {e}") from e

        with self._lock:
            self.stores = stores
            self.sessions = sessions
        logger.info(f"Restored {len(stores)} users from the database")

    # ──────────────────────────────────────────────
    # READS  (local state; no Raft round trip here)
    # ──────────────────────────────────────────────

    def get_store(self, username: str) -> Optional[Store]:
        """The user's Store — from RAM, or loaded from MySQL on a miss."""
        with self._lock:
            store = self.stores.get(username)
        if store is not None or not self.use_db:
            return store
        return self._load_user_from_db(username)

    def user_exists(self, username: str) -> bool:
        return self.get_store(username) is not None

    def verify_password(self, username: str, password: str) -> bool:
        store = self.get_store(username)
        return store is not None and store.verify_password(password)

    def get(self, username: str, key: str, at_ms: Optional[int] = None) -> Optional[str]:
        """The value, or None if missing or expired."""
        store = self.get_store(username)
        if store is None:
            return None
        item = self._lookup(store, username, key)
        if item is None or _expired(item[1], at_ms or now_ms()):
            return None
        return item[0]

    def has(self, username: str, key: str) -> bool:
        return self.get(username, key) is not None

    def ttl(self, username: str, key: str) -> int:
        """Seconds left; -1 = no expiry; -2 = no such key (Redis semantics)."""
        store = self.get_store(username)
        item = self._lookup(store, username, key) if store else None
        now = now_ms()
        if item is None or _expired(item[1], now):
            return -2
        if item[1] is None:
            return -1
        return -(-(item[1] - now) // 1000)      # round up

    def keys(self, username: str, pattern: Optional[str] = None) -> list[str]:
        """The user's live keys, sorted, optionally filtered by a glob pattern."""
        store = self.get_store(username)
        if store is None:
            return []
        now = now_ms()
        if self.use_db:
            from apps.users.db_service import list_keys
            names = list_keys(username, now)
        else:
            with store.lock:
                names = sorted(k for k, _ in store.cache.items()
                               if not _expired(store.expiry.get(k), now))
        if pattern:
            names = [k for k in names if fnmatch.fnmatchcase(k, pattern)]
        return names

    def next_expiry(self) -> Optional[int]:
        """Earliest expiry time of any key (the leader sweeps once it passes)."""
        if self.use_db:
            from apps.users.db_service import next_expiry
            return next_expiry()
        with self._lock:
            stores = list(self.stores.values())
        earliest = None
        for store in stores:
            with store.lock:
                if store.expiry:
                    first = min(store.expiry.values())
                    earliest = first if earliest is None else min(earliest, first)
        return earliest

    def session_result(self, request_id: str) -> Any:
        """What an already-applied request returned, or None if unknown."""
        client_id, seq = _parse_request_id(request_id)
        with self._lock:
            session = self.sessions.get(client_id)
        if session is not None and session[0] == seq:
            return session[1]
        return None

    # ──────────────────────────────────────────────
    # WRITES  (only ever called by Raft, in log order, on every node)
    # ──────────────────────────────────────────────

    def apply(self, entry: LogEntry) -> Any:
        """
        Apply one committed log entry and return its result:

            SIGNUP <password_hash>          → True if created, False if taken
            SET <key> <value>               → True (False: unknown user)
            SETEX <key> <expire_at> <value> → True
            DELETE <key> [<now>]            → True if the key existed
            EXPIREAT <key> <expire_at> <now>→ True if the key existed
            PERSIST <key> <now>             → True if an expiry was removed
            SWEEP <now>                     → number of expired keys removed

        The database is written BEFORE RAM: if MySQL fails, Raft retries
        the entry and RAM never shows a value MySQL doesn't have.
        """
        if entry.request_id:
            cached = self._check_session(entry.request_id)
            if cached is not _MISS:
                return cached
        result = self._apply_command(entry)
        if entry.request_id:
            self._record_session(entry.request_id, result, entry.index)
        return result

    def _apply_command(self, entry: LogEntry) -> Any:
        verb, _, rest = entry.command.partition(" ")
        user = entry.username
        if verb == "SIGNUP":
            return self._apply_signup(user, rest)
        if verb == "SET":
            key, _, value = rest.partition(" ")
            return self._apply_set(user, key, value, None)
        if verb == "SETEX":
            key, expire_at, value = rest.split(" ", 2)
            return self._apply_set(user, key, value, int(expire_at))
        if verb == "DELETE":
            key, _, now = rest.partition(" ")
            return self._apply_delete(user, key, int(now) if now else None)
        if verb == "EXPIREAT":
            key, expire_at, now = rest.split(" ")
            return self._apply_expiry(user, key, int(expire_at), int(now))
        if verb == "PERSIST":
            key, now = rest.split(" ")
            return self._apply_expiry(user, key, None, int(now))
        if verb == "SWEEP":
            return self._apply_sweep(int(rest))
        logger.warning(f"Ignoring unknown log command {verb!r}")
        return None

    def _apply_signup(self, username: str, password_hash: str) -> bool:
        # Two people racing for the same name both get a SIGNUP into the
        # log — the one that comes FIRST in the log wins, on every node.
        if self.get_store(username) is not None:
            return False
        if self.use_db:
            from apps.users.db_service import save_user
            save_user(username, password_hash)
        with self._lock:
            self.stores[username] = Store(username, password_hash, capacity=self.capacity)
        return True

    def _apply_set(self, username: str, key: str, value: str, expire_at: Optional[int]) -> bool:
        store = self.get_store(username)
        if store is None:
            logger.warning(f"SET for unknown user '{username}' ignored")
            return False
        with store.lock:
            if self.use_db:
                from apps.users.db_service import save_entry
                if expire_at is None:
                    save_entry(username, key, value)
                else:
                    save_entry(username, key, value, expire_at=expire_at)
            store.cache.set(key, value)
            if expire_at is None:
                store.expiry.pop(key, None)
            else:
                store.expiry[key] = expire_at
        return True

    def _apply_delete(self, username: str, key: str, at_ms: Optional[int]) -> bool:
        store = self.get_store(username)
        if store is None:
            return False
        with store.lock:
            # The key may exist only in MySQL (evicted from RAM), so look
            # it up properly; an expired key counts as already gone.
            item = self._lookup(store, username, key)
            existed = item is not None and not (at_ms is not None and _expired(item[1], at_ms))
            if self.use_db:
                from apps.users.db_service import delete_entry
                delete_entry(username, key)
            store.cache.delete(key)
            store.expiry.pop(key, None)
        return existed

    def _apply_expiry(self, username: str, key: str, expire_at: Optional[int], at_ms: int) -> bool:
        store = self.get_store(username)
        if store is None:
            return False
        with store.lock:
            item = self._lookup(store, username, key)
            if item is None or _expired(item[1], at_ms):
                return False
            if expire_at is None and item[1] is None:
                return False                    # PERSIST on a key without expiry
            if self.use_db:
                from apps.users.db_service import set_expiry
                set_expiry(username, key, expire_at)
            if expire_at is None:
                store.expiry.pop(key, None)
            else:
                store.expiry[key] = expire_at
        return True

    def _apply_sweep(self, at_ms: int) -> int:
        removed = 0
        if self.use_db:
            from apps.users.db_service import delete_expired
            removed = delete_expired(at_ms)
        with self._lock:
            stores = list(self.stores.values())
        for store in stores:
            with store.lock:
                for key, expire_at in list(store.expiry.items()):
                    if expire_at <= at_ms:
                        del store.expiry[key]
                        if store.cache.delete(key) and not self.use_db:
                            removed += 1
        return removed

    # ──────────────────────────────────────────────
    # SESSIONS
    # ──────────────────────────────────────────────

    def _check_session(self, request_id: str) -> Any:
        client_id, seq = _parse_request_id(request_id)
        with self._lock:
            session = self.sessions.get(client_id)
        if session is None or seq > session[0]:
            return _MISS                 # new request
        if seq == session[0]:
            return session[1]            # a retry — same answer as before
        return None                      # an even older duplicate: ignore it

    def _record_session(self, request_id: str, result: Any, index: int):
        client_id, seq = _parse_request_id(request_id)
        with self._lock:
            self.sessions[client_id] = (seq, result, index)
            evict = None
            if len(self.sessions) > MAX_SESSIONS:
                # Deterministic on every node: the session used longest ago
                evict = min(self.sessions, key=lambda c: self.sessions[c][2])
                del self.sessions[evict]
        if self.use_db:
            from apps.users.db_service import save_session, delete_session
            save_session(client_id, seq, json.dumps(result), index)
            if evict:
                delete_session(evict)

    # ──────────────────────────────────────────────
    # SNAPSHOTS  (a follower that fell too far behind)
    # ──────────────────────────────────────────────

    def snapshot(self, out):
        """Write everything to `out` as JSON lines (see db_service.dump_to)."""
        if self.use_db:
            from apps.users.db_service import dump_to
            dump_to(out)
            return
        with self._lock:
            stores = list(self.stores.values())
            sessions = list(self.sessions.items())
        for s in stores:
            out.write(json.dumps({"u": s.username, "h": s.password_hash}) + "\n")
        for s in stores:
            with s.lock:
                rows = [[s.username, k, v, s.expiry.get(k)] for k, v in s.cache.items()]
            for row in rows:
                out.write(json.dumps({"e": row}) + "\n")
        for client_id, (seq, result, index) in sessions:
            out.write(json.dumps({"s": [client_id, seq, json.dumps(result), index]}) + "\n")

    def restore(self, source):
        """Throw away our data and load the leader's snapshot instead."""
        if self.use_db:
            from apps.users.db_service import replace_from
            replace_from(source)
            self.load_from_db()
            return
        stores, sessions = {}, {}
        for line in source:
            if not line.strip():
                continue
            row = json.loads(line)
            if "u" in row:
                stores[row["u"]] = Store(row["u"], row["h"], capacity=self.capacity)
            elif "e" in row:
                username, key, value, expire_at = row["e"]
                store = stores[username]
                store.cache.set(key, value)
                if expire_at is not None:
                    store.expiry[key] = expire_at
            elif "s" in row:
                client_id, seq, result, index = row["s"]
                sessions[client_id] = (seq, json.loads(result), index)
        with self._lock:
            self.stores = stores
            self.sessions = sessions

    # ──────────────────────────────────────────────

    def _lookup(self, store: Store, username: str, key: str) -> Optional[tuple[str, Optional[int]]]:
        """(value, expire_at) from RAM, or from MySQL on a miss (warming RAM)."""
        with store.lock:
            value = store.cache.get(key)
            if value is not None:
                return value, store.expiry.get(key)
            if not self.use_db:
                return None
            from apps.users.db_service import get_entry_with_expiry
            row = get_entry_with_expiry(username, key)
            if row is None:
                return None
            value, expire_at = row
            store.cache.set(key, value)
            if expire_at is None:
                store.expiry.pop(key, None)
            else:
                store.expiry[key] = expire_at
            return value, expire_at

    def _load_user_from_db(self, username: str) -> Optional[Store]:
        from apps.users.db_service import get_user, get_all_entries, get_expiries
        db_user = get_user(username)
        if db_user is None:
            return None
        store = Store(username, db_user.password_hash, capacity=self.capacity)
        entries = get_all_entries(username, limit=self.capacity)
        for key, value in entries.items():
            store.cache.set(key, value)
        store.expiry = {k: exp for k, exp in get_expiries(username).items() if k in entries}
        with self._lock:
            return self.stores.setdefault(username, store)


def _expired(expire_at: Optional[int], at_ms: int) -> bool:
    return expire_at is not None and expire_at <= at_ms


def _parse_request_id(request_id: str) -> tuple[str, int]:
    client_id, _, seq = request_id.rpartition(":")
    return client_id, int(seq)
