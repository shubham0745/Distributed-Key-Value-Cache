"""
apps/users/db_service.py

All database operations in ONE place.
The TCP server and Store never import Django models directly —
they go through this service. This keeps the DB logic separate
from the network/cache logic.

Functions:
    save_user()         → insert new CacheUser row
    get_user()          → fetch CacheUser by username
    user_exists()       → quick existence check
    save_entry()        → upsert a key-value pair
    delete_entry()      → remove a key-value pair
    get_all_entries()   → load all entries for a user
    load_all_users()    → load every user on startup
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def save_user(username: str, password_hash: str) -> "CacheUser":
    """
    Create a new user in MySQL.
    Called during SIGNUP after we've verified the username is free.
    """
    from apps.users.models import CacheUser
    user = CacheUser.objects.create(
        username=username,
        password_hash=password_hash
    )
    logger.info(f"DB: created user '{username}'")
    return user


def get_user(username: str) -> Optional["CacheUser"]:
    """
    Fetch a CacheUser row by username.
    Returns None if not found.
    """
    from apps.users.models import CacheUser
    try:
        return CacheUser.objects.get(username=username)
    except CacheUser.DoesNotExist:
        return None


def user_exists(username: str) -> bool:
    """Quick check — does this username exist in MySQL?"""
    from apps.users.models import CacheUser
    return CacheUser.objects.filter(username=username).exists()


def save_entry(username: str, key: str, value: str) -> None:
    """
    Upsert (update or insert) a cache entry for a user.
    
    'upsert' means:
      - if (user, key) row EXISTS → update the value
      - if it DOESN'T exist       → create it
    
    Django's update_or_create() does this in a single SQL statement.
    This is called every time a user does SET key value.
    """
    from apps.users.models import CacheUser, CacheEntry
    try:
        user = CacheUser.objects.get(username=username)
        CacheEntry.objects.update_or_create(
            user=user,
            cache_key=key,
            defaults={"cache_value": value}
        )
        logger.debug(f"DB: saved entry [{username}] {key}={value}")
    except CacheUser.DoesNotExist:
        logger.error(f"DB: save_entry failed — user '{username}' not found")


def delete_entry(username: str, key: str) -> bool:
    """
    Delete a cache entry. Returns True if deleted, False if not found.
    Called every time a user does DELETE key.
    """
    from apps.users.models import CacheUser, CacheEntry
    try:
        user = CacheUser.objects.get(username=username)
        deleted_count, _ = CacheEntry.objects.filter(
            user=user,
            cache_key=key
        ).delete()
        return deleted_count > 0
    except CacheUser.DoesNotExist:
        return False


def get_all_entries(username: str) -> dict[str, str]:
    """
    Load ALL cache entries for a user from MySQL.
    Returns a dict of {key: value}.
    Called on startup to restore a user's cache from DB.
    """
    from apps.users.models import CacheUser, CacheEntry
    try:
        user = CacheUser.objects.get(username=username)
        entries = CacheEntry.objects.filter(user=user)
        return {e.cache_key: e.cache_value for e in entries}
    except CacheUser.DoesNotExist:
        return {}


def load_all_users() -> list[dict]:
    """
    Load ALL users from MySQL on server startup.
    Returns a list of dicts with username and password_hash.
    
    This is called ONCE when the server starts so it can
    rebuild the in-memory stores dict from persisted data.
    """
    from apps.users.models import CacheUser
    users = CacheUser.objects.all()
    result = []
    for u in users:
        result.append({
            "username": u.username,
            "password_hash": u.password_hash,
        })
    logger.info(f"DB: loaded {len(result)} users from MySQL")
    return result