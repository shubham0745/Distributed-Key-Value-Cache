"""
Full test suite for Week 1: Cache Engine
Run with: pytest tests/test_cache.py -v
"""
import threading
import time
import pytest

from cache.cache_impl import Cache
from cache.lru_cache import LRUCache
from cache import CacheFactory


# ──────────────────────────────────────────────
# BASIC CACHE TESTS
# ──────────────────────────────────────────────

class TestCacheBasicOperations:
    """Test all CRUD operations on the basic Cache."""

    def setup_method(self):
        self.cache = Cache()

    def test_set_and_get(self):
        self.cache.set("name", "shubham")
        assert self.cache.get("name") == "shubham"

    def test_get_nonexistent_returns_none(self):
        assert self.cache.get("doesnotexist") is None

    def test_has_existing_key(self):
        self.cache.set("city", "gurugram")
        assert self.cache.has("city") is True

    def test_has_nonexistent_key(self):
        assert self.cache.has("ghost") is False

    def test_delete_existing_key(self):
        self.cache.set("temp", "value")
        result = self.cache.delete("temp")
        assert result is True
        assert self.cache.has("temp") is False
        assert self.cache.get("temp") is None

    def test_delete_nonexistent_returns_false(self):
        result = self.cache.delete("nope")
        assert result is False

    def test_overwrite_existing_key(self):
        self.cache.set("key", "first")
        self.cache.set("key", "second")
        assert self.cache.get("key") == "second"

    def test_size_empty_cache(self):
        assert self.cache.size() == 0

    def test_size_after_operations(self):
        self.cache.set("a", "1")
        self.cache.set("b", "2")
        self.cache.set("c", "3")
        assert self.cache.size() == 3
        self.cache.delete("b")
        assert self.cache.size() == 2

    def test_clear(self):
        self.cache.set("x", "1")
        self.cache.set("y", "2")
        self.cache.clear()
        assert self.cache.size() == 0
        assert self.cache.get("x") is None

    def test_empty_key_raises(self):
        with pytest.raises(ValueError):
            self.cache.set("", "value")

    def test_non_string_key_raises(self):
        with pytest.raises(TypeError):
            self.cache.set(123, "value")  # type: ignore

    def test_non_string_value_raises(self):
        with pytest.raises(TypeError):
            self.cache.set("key", 456)  # type: ignore


# ──────────────────────────────────────────────
# THREAD SAFETY TESTS
# ──────────────────────────────────────────────

class TestCacheThreadSafety:
    """Verify the cache is safe to use from multiple threads simultaneously."""

    def test_concurrent_writes_no_data_loss(self):
        """100 threads each write their own key. All must survive."""
        cache = Cache()
        num_threads = 100

        def writer(thread_id):
            cache.set(f"key_{thread_id}", f"value_{thread_id}")

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert cache.size() == num_threads
        for i in range(num_threads):
            assert cache.get(f"key_{i}") == f"value_{i}"

    def test_concurrent_reads_no_crash(self):
        """Multiple readers reading the same key simultaneously should be fine."""
        cache = Cache()
        cache.set("shared", "data")
        results = []

        def reader():
            results.append(cache.get("shared"))

        threads = [threading.Thread(target=reader) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r == "data" for r in results)

    def test_concurrent_read_write_no_corruption(self):
        """Mixed reads and writes simultaneously. No crash, no corruption."""
        cache = Cache()
        errors = []

        def writer():
            for i in range(100):
                try:
                    cache.set(f"k{i}", f"v{i}")
                except Exception as e:
                    errors.append(e)

        def reader():
            for i in range(100):
                try:
                    cache.get(f"k{i}")  # May be None, that's fine
                except Exception as e:
                    errors.append(e)

        threads = (
            [threading.Thread(target=writer) for _ in range(5)] +
            [threading.Thread(target=reader) for _ in range(5)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Thread errors: {errors}"


# ──────────────────────────────────────────────
# LRU CACHE TESTS
# ──────────────────────────────────────────────

class TestLRUCacheBasicOperations:
    """Test LRUCache behaves like a regular cache for non-eviction scenarios."""

    def setup_method(self):
        self.cache = LRUCache(capacity=10)

    def test_set_and_get(self):
        self.cache.set("hello", "world")
        assert self.cache.get("hello") == "world"

    def test_get_missing_returns_none(self):
        assert self.cache.get("missing") is None

    def test_has(self):
        self.cache.set("x", "1")
        assert self.cache.has("x") is True
        assert self.cache.has("y") is False

    def test_delete(self):
        self.cache.set("del", "me")
        assert self.cache.delete("del") is True
        assert self.cache.has("del") is False

    def test_delete_missing(self):
        assert self.cache.delete("nope") is False

    def test_clear(self):
        self.cache.set("a", "1")
        self.cache.set("b", "2")
        self.cache.clear()
        assert self.cache.size() == 0

    def test_overwrite(self):
        self.cache.set("key", "old")
        self.cache.set("key", "new")
        assert self.cache.get("key") == "new"
        assert self.cache.size() == 1  # Still one key, not two


class TestLRUEviction:
    """Test the actual LRU eviction behaviour — the core of this class."""

    def test_evicts_when_full(self):
        """When capacity is exceeded, the oldest/LRU key is removed."""
        cache = LRUCache(capacity=3)
        cache.set("a", "1")
        cache.set("b", "2")
        cache.set("c", "3")
        # Cache is full: [a, b, c]
        cache.set("d", "4")
        # 'a' should be evicted (LRU), d is added
        assert cache.has("a") is False
        assert cache.has("b") is True
        assert cache.has("c") is True
        assert cache.has("d") is True

    def test_get_refreshes_lru_order(self):
        """Accessing a key via get() moves it to 'most recently used'."""
        cache = LRUCache(capacity=3)
        cache.set("a", "1")
        cache.set("b", "2")
        cache.set("c", "3")
        # Access 'a' — now LRU order is [b, c, a]
        cache.get("a")
        # Add 'd' — 'b' should be evicted (now LRU), NOT 'a'
        cache.set("d", "4")
        assert cache.has("a") is True
        assert cache.has("b") is False  # b was evicted
        assert cache.has("c") is True
        assert cache.has("d") is True

    def test_set_existing_key_refreshes_order(self):
        """Updating an existing key also refreshes its LRU position."""
        cache = LRUCache(capacity=3)
        cache.set("a", "1")
        cache.set("b", "2")
        cache.set("c", "3")
        # Update 'a' — moves it to end, 'b' is now LRU
        cache.set("a", "updated")
        cache.set("d", "4")
        assert cache.has("a") is True  # was refreshed
        assert cache.has("b") is False  # b evicted
        assert cache.get("a") == "updated"

    def test_eviction_count_increments(self):
        cache = LRUCache(capacity=2)
        cache.set("a", "1")
        cache.set("b", "2")
        assert cache.eviction_count() == 0
        cache.set("c", "3")  # evicts 'a'
        assert cache.eviction_count() == 1
        cache.set("d", "4")  # evicts 'b'
        assert cache.eviction_count() == 2

    def test_peek_lru(self):
        cache = LRUCache(capacity=5)
        cache.set("first", "1")
        cache.set("second", "2")
        cache.set("third", "3")
        assert cache.peek_lru() == "first"
        cache.get("first")  # refresh first
        assert cache.peek_lru() == "second"

    def test_peek_mru(self):
        cache = LRUCache(capacity=5)
        cache.set("a", "1")
        cache.set("b", "2")
        assert cache.peek_mru() == "b"
        cache.get("a")
        assert cache.peek_mru() == "a"

    def test_capacity_one(self):
        """Edge case: capacity of 1 should always evict on every new key."""
        cache = LRUCache(capacity=1)
        cache.set("a", "1")
        cache.set("b", "2")
        assert cache.has("a") is False
        assert cache.get("b") == "2"

    def test_invalid_capacity_raises(self):
        with pytest.raises(ValueError):
            LRUCache(capacity=0)
        with pytest.raises(ValueError):
            LRUCache(capacity=-5)


class TestLRUThreadSafety:
    """LRU must also be thread safe."""

    def test_concurrent_sets_respect_capacity(self):
        capacity = 50
        cache = LRUCache(capacity=capacity)
        num_threads = 200

        def writer(i):
            cache.set(f"key_{i}", f"value_{i}")

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Cache must never exceed its capacity
        assert cache.size() <= capacity

    def test_concurrent_eviction_no_crash(self):
        cache = LRUCache(capacity=10)
        errors = []

        def stress():
            try:
                for i in range(100):
                    cache.set(f"k{i}", f"v{i}")
                    cache.get(f"k{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=stress) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert cache.size() <= 10


# ──────────────────────────────────────────────
# CACHE FACTORY TESTS
# ──────────────────────────────────────────────

class TestCacheFactory:

    def test_create_basic_cache(self):
        cache = CacheFactory.create("basic")
        assert isinstance(cache, Cache)

    def test_create_lru_cache(self):
        cache = CacheFactory.create("lru", capacity=100)
        assert isinstance(cache, LRUCache)

    def test_created_cache_implements_interface(self):
        from cache.cache_interface import ICache
        for cache_type in ["basic", "lru"]:
            cache = CacheFactory.create(cache_type)
            assert isinstance(cache, ICache)

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown cache type"):
            CacheFactory.create("redis")

    def test_default_type_is_lru(self):
        cache = CacheFactory.create()
        assert isinstance(cache, LRUCache)