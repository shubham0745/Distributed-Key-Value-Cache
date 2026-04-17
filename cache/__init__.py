from cache.cache_interface import ICache
from cache.cache_impl import Cache
from cache.lru_cache import LRUCache


class CacheFactory:
    """
    Factory to create cache instances by type.
    
    WHY A FACTORY?
    The rest of the system (store, server) should NOT need to know
    which cache implementation they're getting. They just ask for
    "a cache" and the factory decides. This is the same pattern as
    Go's interface — code to the interface, not the implementation.
    
    Usage:
        cache = CacheFactory.create("lru", capacity=500)
        cache = CacheFactory.create("basic")
    """

    CACHE_TYPES = {
        "basic": Cache,
        "lru": LRUCache,
    }

    @classmethod
    def create(cls, cache_type: str = "lru", capacity: int = 1000) -> ICache:
        """
        Create and return a cache instance.
        
        Args:
            cache_type: "basic" or "lru"
            capacity:   Max keys for LRU (ignored for basic)
        
        Returns:
            An ICache instance
        
        Raises:
            ValueError: if cache_type is unknown
        """
        cache_type = cache_type.lower()
        if cache_type not in cls.CACHE_TYPES:
            raise ValueError(
                f"Unknown cache type '{cache_type}'. "
                f"Available: {list(cls.CACHE_TYPES.keys())}"
            )

        if cache_type == "lru":
            return LRUCache(capacity=capacity)
        return Cache()