from cache import CacheFactory
from cache.cache_interface import ICache


class Store:
    """
    Per-user store. Every user who logs in gets their OWN isolated Store.
    
    This mirrors store_impl.go from the original project.
    
    WHY PER-USER ISOLATION?
    Without this, user A could do GET on user B's keys.
    With namespacing: shubham's "name" key and rahul's "name" key
    are completely separate — they never collide.
    
    Structure:
        server.stores = {
            "shubham": Store(username="shubham", cache=LRUCache),
            "rahul":   Store(username="rahul",   cache=LRUCache),
        }
    
    The password is stored HERE (hashed) so the server can verify
    login attempts without hitting MySQL every single time.
    In Week 3 we'll add MySQL as the persistent backup.
    """

    def __init__(self, username: str, password_hash: str, capacity: int = 1000):
        self.username = username
        self._password_hash = password_hash  # Never stored as plain text
        self.cache: ICache = CacheFactory.create("lru", capacity=capacity)

    def verify_password(self, plain_password: str) -> bool:
        """
        Check if a plain password matches the stored hash.
        Uses Django's check_password — same as any Django login.
        We import it here so the store stays decoupled from the server.
        """
        from django.contrib.auth.hashers import check_password
        return check_password(plain_password, self._password_hash)

    def __repr__(self) -> str:
        return f"Store(user={self.username}, cache_size={self.cache.size()})"