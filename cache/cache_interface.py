from abc import ABC, abstractmethod
from typing import Optional


class ICache(ABC):
    """
    Abstract base class defining the contract for all cache implementations.
    Every cache (basic, LRU) MUST implement these methods.
    """

    @abstractmethod
    def set(self, key: str, value: str) -> None:
        """Store a key-value pair. Overwrites if key exists."""
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[str]:
        """Return value for key, or None if not found."""
        pass

    @abstractmethod
    def has(self, key: str) -> bool:
        """Return True if key exists in cache."""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if deleted, False if key didn't exist."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Remove all keys from the cache."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Return number of keys currently in cache."""
        pass

    @abstractmethod
    def items(self) -> list[tuple[str, str]]:
        """Return a copy of every (key, value) pair. Used for Raft snapshots."""
        pass