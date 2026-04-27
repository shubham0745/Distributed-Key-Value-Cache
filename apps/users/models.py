"""
apps/users/models.py

Two Django models:
  1. CacheUser  — stores username + hashed password
  2. CacheEntry — stores every key-value pair per user

WHY TWO TABLES?
  CacheUser   → "who can log in"
  CacheEntry  → "what data they have stored"

On server start we load BOTH tables into RAM.
On every SET/DELETE we write to BOTH RAM and MySQL.
This is called Write-Through caching.
"""
from django.db import models


class CacheUser(models.Model):
    """
    Represents a registered user of the cache server.
    
    Fields:
        username     — unique identifier, used for login
        password_hash— PBKDF2 hashed password (never plain text)
        created_at   — when the user registered
    """
    username      = models.CharField(max_length=150, unique=True)
    password_hash = models.CharField(max_length=255)
    created_at    = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "cache_users"

    def __str__(self):
        return self.username


class CacheEntry(models.Model):
    """
    Represents one key-value pair belonging to a user.
    
    Fields:
        user        — which user owns this key (FK to CacheUser)
        cache_key   — the key string (e.g. "name", "city")
        cache_value — the value string (e.g. "shubham", "gurugram")
        updated_at  — last time this entry was SET

    The combination of (user, cache_key) is unique —
    one user can't have two entries with the same key.
    """
    user        = models.ForeignKey(CacheUser, on_delete=models.CASCADE,
                                    related_name="entries")
    cache_key   = models.CharField(max_length=512)
    cache_value = models.TextField()
    updated_at  = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "cache_entries"
        unique_together = ("user", "cache_key")   # one key per user

    def __str__(self):
        return f"{self.user.username}:{self.cache_key}"