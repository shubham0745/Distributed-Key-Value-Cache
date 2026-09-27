"""
server/state_machine.py  (Week 5)

The "state machine" in Raft terms: the data every node must agree on.
For us that is which users exist (with password hashes) and each
user's key/values.

Raft guarantees every node calls apply() with the SAME entries in the
SAME order. apply() is deterministic, so every node ends up with the
same data. Reads never go through Raft — they read local state.

Storage layers when use_db=True:
    RAM   — one LRUCache per user (fast, holds at most `capacity` keys)
    MySQL — cache_users / cache_entries (durable, unbounded)
A RAM miss falls back to MySQL and warms the cache again, so an LRU
eviction never loses data. With use_db=False RAM is the only copy and
an eviction really forgets the key — a pure cache.
"""
import logging
import threading
from typing import Any, Optional

from raft.types import LogEntry
from store.store import Store

logger = logging.getLogger(__name__)


class StateLoadError(RuntimeError):
    """The database could not be read at startup."""


class CacheStateMachine:

    def __init__(self, use_db: bool = True, capacity: int = 1000):
        self.use_db = use_db
        self.capacity = capacity
        self.stores: dict[str, Store] = {}
        self._lock = threading.RLock()     # guards the stores dict itself

    # ──────────────────────────────────────────────
    # STARTUP
    # ──────────────────────────────────────────────

    def load_from_db(self):
        """
        Rebuild the in-memory stores from MySQL: every user, plus each
        user's most recent `capacity` keys (older ones load on demand).

        Raises StateLoadError instead of starting empty — an empty RAM
        view over a full database would reject valid logins and let
        SIGNUP re-create existing users.
        """
        try:
            from apps.users.db_service import load_all_users, get_all_entries
            users = load_all_users()
            stores = {}
            for u in users:
                store = Store(u["username"], u["password_hash"], capacity=self.capacity)
                # Restore their cache entries
                for key, value in get_all_entries(u["username"], limit=self.capacity).items():
                    store.cache.set(key, value)
                stores[u["username"]] = store
        except Exception as e:
            raise StateLoadError(f"could not load data from the database: {e}") from e

        with self._lock:
            self.stores = stores
        logger.info(f"Restored {len(stores)} users from the database")

    # ──────────────────────────────────────────────
    # READS  (served by any node, no Raft round trip)
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

    def get(self, username: str, key: str) -> Optional[str]:
        store = self.get_store(username)
        if store is None:
            return None
        value = store.cache.get(key)
        if value is not None or not self.use_db:
            return value

        # RAM miss — maybe the LRU evicted it. Ask MySQL and warm RAM.
        with store.lock:
            value = store.cache.get(key)       # apply() may have filled it meanwhile
            if value is None:
                from apps.users.db_service import get_entry
                value = get_entry(username, key)
                if value is not None:
                    store.cache.set(key, value)
        return value

    def has(self, username: str, key: str) -> bool:
        store = self.get_store(username)
        if store is None:
            return False
        if store.cache.has(key):
            return True
        return self.use_db and self.get(username, key) is not None

    # ──────────────────────────────────────────────
    # WRITES  (only ever called by Raft, in log order, on every node)
    # ──────────────────────────────────────────────

    def apply(self, entry: LogEntry) -> Any:
        """
        Apply one committed log entry.

            SIGNUP <password_hash> → True if created, False if name taken
            SET <key> <value>      → True
            DELETE <key>           → True if the key existed

        The database is written BEFORE RAM: if MySQL fails, Raft retries
        the entry and RAM never shows a value MySQL doesn't have.
        """
        verb, _, rest = entry.command.partition(" ")
        if verb == "SIGNUP":
            return self._apply_signup(entry.username, rest)
        if verb == "SET":
            key, _, value = rest.partition(" ")
            return self._apply_set(entry.username, key, value)
        if verb == "DELETE":
            return self._apply_delete(entry.username, rest)
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

    def _apply_set(self, username: str, key: str, value: str) -> bool:
        store = self.get_store(username)
        if store is None:
            logger.warning(f"SET for unknown user '{username}' ignored")
            return False
        with store.lock:
            if self.use_db:
                from apps.users.db_service import save_entry
                save_entry(username, key, value)
            store.cache.set(key, value)
        return True

    def _apply_delete(self, username: str, key: str) -> bool:
        store = self.get_store(username)
        if store is None:
            return False
        with store.lock:
            # Always delete from MySQL — the key may exist there even
            # after the LRU evicted it from RAM.
            in_db = False
            if self.use_db:
                from apps.users.db_service import delete_entry
                in_db = delete_entry(username, key)
            in_ram = store.cache.delete(key)
        return bool(in_db or in_ram)

    # ──────────────────────────────────────────────
    # SNAPSHOTS  (a follower that fell too far behind)
    # ──────────────────────────────────────────────

    def snapshot(self) -> dict:
        """Everything, as plain JSON-friendly data."""
        if self.use_db:
            from apps.users.db_service import dump_all
            return {"users": dump_all()}
        with self._lock:
            stores = list(self.stores.values())
        return {"users": [
            {"username": s.username, "password_hash": s.password_hash,
             "entries": [[k, v] for k, v in s.cache.items()]}
            for s in stores
        ]}

    def restore(self, data: Optional[dict]):
        """Throw away our data and take the leader's snapshot instead."""
        users = (data or {}).get("users", [])
        if self.use_db:
            from apps.users.db_service import replace_all
            replace_all(users)
            self.load_from_db()
            return
        stores = {}
        for u in users:
            store = Store(u["username"], u["password_hash"], capacity=self.capacity)
            for key, value in u["entries"]:
                store.cache.set(key, value)
            stores[u["username"]] = store
        with self._lock:
            self.stores = stores

    # ──────────────────────────────────────────────

    def _load_user_from_db(self, username: str) -> Optional[Store]:
        from apps.users.db_service import get_user, get_all_entries
        db_user = get_user(username)
        if db_user is None:
            return None
        store = Store(username, db_user.password_hash, capacity=self.capacity)
        for key, value in get_all_entries(username, limit=self.capacity).items():
            store.cache.set(key, value)
        with self._lock:
            return self.stores.setdefault(username, store)
