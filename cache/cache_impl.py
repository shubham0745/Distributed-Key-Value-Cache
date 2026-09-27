import threading
from typing import Optional

from cache.cache_interface import ICache


class Cache(ICache):
    """
    Thread-safe in-memory key-value cache using a plain dict.
    This directly mirrors cache_impl.go from the original project.

    Why a lock at all?
    Every client connection runs in its own thread, so two threads could
    touch the dict at the same moment. threading.RLock lets exactly ONE
    thread inside at a time — readers included. "Reentrant" means the
    same thread may acquire it again without deadlocking itself.

    The original Go code used sync.RWMutex, which also lets many READERS
    in at once. Python's standard library has no read-write lock; with the
    GIL a plain lock costs very little here, so we keep it simple.
    """

    def __init__(self):
        self._data: dict[str, str] = {}
        self._lock = threading.RLock()  # Reentrant read-write lock

    def set(self, key: str, value: str) -> None:
        """
        Store key-value. Thread-safe write operation.
        Go equivalent: c.Lock() / defer c.Unlock()
        """
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError(f"Key and value must be strings, got {type(key)}, {type(value)}")
        if not key:
            raise ValueError("Key cannot be empty")

        with self._lock:
            self._data[key] = value

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve value by key. Thread-safe read operation.
        Returns None if key doesn't exist (Go version returned (val, bool)).
        """
        with self._lock:
            return self._data.get(key, None)

    def has(self, key: str) -> bool:
        """
        Check if key exists without retrieving its value.
        Go equivalent: _, found := c.data[key]
        """
        with self._lock:
            return key in self._data

    def delete(self, key: str) -> bool:
        """
        Remove a key. Returns True if deleted, False if key didn't exist.
        Go equivalent: delete(c.data, key)
        """
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def clear(self) -> None:
        """Remove all keys. Useful for testing and user logout."""
        with self._lock:
            self._data.clear()

    def size(self) -> int:
        """Return number of keys in cache."""
        with self._lock:
            return len(self._data)

    def items(self) -> list[tuple[str, str]]:
        """Return a copy of every (key, value) pair."""
        with self._lock:
            return list(self._data.items())

    def keys(self) -> list[str]:
        """Return all keys. Useful for debugging."""
        with self._lock:
            return list(self._data.keys())

    def __repr__(self) -> str:
        with self._lock:
            return f"Cache(size={len(self._data)})"