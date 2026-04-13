import threading
from collections import OrderedDict
from typing import Optional

from cache.cache_interface import ICache


class LRUCache(ICache):
    """
    Least Recently Used (LRU) Cache with a fixed capacity.
    
    WHY LRU? In a real cache server (like Redis), you can't store
    infinite data in memory. When the cache is full, you need an
    eviction policy. LRU evicts the key that was LEAST RECENTLY USED.
    
    HOW IT WORKS:
    - We use Python's OrderedDict which maintains insertion order
    - On every GET or SET, we move the accessed key to the END (most recent)
    - When capacity is exceeded, we pop from the FRONT (least recent)
    
    Example with capacity=3:
        SET a=1  →  [a]
        SET b=2  →  [a, b]
        SET c=3  →  [a, b, c]
        GET a    →  [b, c, a]  ← 'a' moved to end (recently used)
        SET d=4  →  [c, a, d]  ← 'b' evicted (least recently used)
    
    Thread safety: Same RLock pattern as Cache.
    """

    def __init__(self, capacity: int = 1000):
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")
        self._capacity = capacity
        self._data: OrderedDict[str, str] = OrderedDict()
        self._lock = threading.RLock()
        self._eviction_count = 0  # Track how many times we've evicted

    def set(self, key: str, value: str) -> None:
        """
        Store key-value. If key exists, update and move to end.
        If cache is full, evict LRU before inserting.
        """
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError(f"Key and value must be strings")
        if not key:
            raise ValueError("Key cannot be empty")

        with self._lock:
            if key in self._data:
                # Key exists: update value and move to end (most recent)
                self._data.move_to_end(key)
                self._data[key] = value
            else:
                # New key: check capacity first
                if len(self._data) >= self._capacity:
                    self._evict_lru()
                self._data[key] = value

    def get(self, key: str) -> Optional[str]:
        """
        Get value AND mark as recently used (move to end).
        This is what makes it LRU — accessing a key refreshes it.
        """
        with self._lock:
            if key not in self._data:
                return None
            # Move to end = mark as most recently used
            self._data.move_to_end(key)
            return self._data[key]

    def has(self, key: str) -> bool:
        """Check existence WITHOUT updating LRU order."""
        with self._lock:
            return key in self._data

    def delete(self, key: str) -> bool:
        """Remove a key from cache."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def clear(self) -> None:
        """Remove all keys."""
        with self._lock:
            self._data.clear()

    def size(self) -> int:
        """Current number of keys."""
        with self._lock:
            return len(self._data)

    def capacity(self) -> int:
        """Maximum number of keys allowed."""
        return self._capacity

    def eviction_count(self) -> int:
        """How many keys have been evicted so far."""
        with self._lock:
            return self._eviction_count

    def _evict_lru(self) -> Optional[str]:
        """
        Remove the least recently used key (first item in OrderedDict).
        Called internally when capacity is exceeded.
        Returns the evicted key for logging/testing purposes.
        """
        if not self._data:
            return None
        evicted_key, _ = self._data.popitem(last=False)  # last=False = pop from front
        self._eviction_count += 1
        return evicted_key

    def peek_lru(self) -> Optional[str]:
        """
        Return the least recently used key WITHOUT removing it.
        Useful for testing and monitoring.
        """
        with self._lock:
            if not self._data:
                return None
            return next(iter(self._data))

    def peek_mru(self) -> Optional[str]:
        """Return the most recently used key WITHOUT removing it."""
        with self._lock:
            if not self._data:
                return None
            return next(reversed(self._data))

    def __repr__(self) -> str:
        with self._lock:
            return f"LRUCache(size={len(self._data)}, capacity={self._capacity}, evictions={self._eviction_count})"