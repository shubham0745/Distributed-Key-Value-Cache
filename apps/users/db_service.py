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
    get_entry()         → read one value (RAM cache miss)
    delete_entry()      → remove a key-value pair
    get_all_entries()   → load entries for a user (newest `limit` if given)
    load_all_users()    → load every user on startup
    dump_all()          → every user + entry (Raft snapshot)
    replace_all()       → overwrite everything with a snapshot
    close_connection()  → close this thread's DB connection
"""
import functools
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _db_call(func):
    """
    Refresh stale or broken connections before each call.

    Django normally does this at the start/end of every web request.
    Our threads live much longer than a request (a client can stay
    connected for hours), so without this a connection MySQL closed
    after its idle timeout would fail with "server has gone away".
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        from django.db import close_old_connections
        close_old_connections()
        return func(*args, **kwargs)
    return wrapper


@_db_call
def save_user(username: str, password_hash: str) -> "CacheUser":
    """
    Create a new user in MySQL.
    Called when a SIGNUP entry is applied, after checking the name is free.
    """
    from apps.users.models import CacheUser
    user = CacheUser.objects.create(
        username=username,
        password_hash=password_hash
    )
    logger.info(f"DB: created user '{username}'")
    return user


@_db_call
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


@_db_call
def user_exists(username: str) -> bool:
    """Quick check — does this username exist in MySQL?"""
    from apps.users.models import CacheUser
    return CacheUser.objects.filter(username=username).exists()


@_db_call
def save_entry(username: str, key: str, value: str) -> None:
    """
    Upsert (update or insert) a cache entry for a user.

    'upsert' means:
      - if (user, key) row EXISTS → update the value
      - if it DOESN'T exist       → create it

    Django's update_or_create() does this in a single SQL statement.
    This is called every time a SET entry is applied.
    """
    from apps.users.models import CacheUser, CacheEntry
    try:
        user = CacheUser.objects.get(username=username)
        CacheEntry.objects.update_or_create(
            user=user,
            cache_key=key,
            defaults={"cache_value": value}
        )
        logger.debug(f"DB: saved entry [{username}] {key}")
    except CacheUser.DoesNotExist:
        logger.error(f"DB: save_entry failed, user '{username}' not found")


@_db_call
def get_entry(username: str, key: str) -> Optional[str]:
    """
    Read one value straight from MySQL.
    Used when the key is not in RAM — e.g. the LRU evicted it.
    """
    from apps.users.models import CacheEntry
    return (CacheEntry.objects
            .filter(user__username=username, cache_key=key)
            .values_list("cache_value", flat=True)
            .first())


@_db_call
def delete_entry(username: str, key: str) -> bool:
    """
    Delete a cache entry. Returns True if deleted, False if not found.
    Called every time a DELETE entry is applied.
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


@_db_call
def get_all_entries(username: str, limit: Optional[int] = None) -> dict[str, str]:
    """
    Load cache entries for a user from MySQL as {key: value}.

    With `limit`, only the `limit` most recently written entries are
    returned, oldest first — inserting them in that order into an
    LRUCache of that capacity keeps the newest ones as "most recent".
    Called on startup to warm a user's cache from DB.
    """
    from apps.users.models import CacheUser, CacheEntry
    try:
        user = CacheUser.objects.get(username=username)
        entries = CacheEntry.objects.filter(user=user)
        if limit is not None:
            newest = entries.order_by("-updated_at", "-id")[:limit]
            entries = reversed(list(newest))
        return {e.cache_key: e.cache_value for e in entries}
    except CacheUser.DoesNotExist:
        return {}


@_db_call
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


@_db_call
def dump_all() -> list[dict]:
    """
    Every user with ALL their entries — the Raft snapshot of this node.
    Shape: [{"username", "password_hash", "entries": [[key, value], ...]}]
    """
    from apps.users.models import CacheUser, CacheEntry
    users = {u.id: {"username": u.username, "password_hash": u.password_hash, "entries": []}
             for u in CacheUser.objects.all()}
    for e in CacheEntry.objects.order_by("updated_at", "id"):
        if e.user_id in users:
            users[e.user_id]["entries"].append([e.cache_key, e.cache_value])
    return list(users.values())


@_db_call
def replace_all(users: list[dict]) -> None:
    """
    Overwrite every user and entry with a snapshot from the leader
    (same shape as dump_all). All-or-nothing, in one transaction.
    """
    from django.db import transaction
    from apps.users.models import CacheUser, CacheEntry
    with transaction.atomic():
        CacheEntry.objects.all().delete()
        CacheUser.objects.all().delete()
        created = CacheUser.objects.bulk_create([
            CacheUser(username=u["username"], password_hash=u["password_hash"])
            for u in users
        ])
        # bulk_create doesn't return ids on every backend — look them up
        ids = dict(CacheUser.objects.values_list("username", "id"))
        CacheEntry.objects.bulk_create([
            CacheEntry(user_id=ids[u["username"]], cache_key=key, cache_value=value)
            for u in users
            for key, value in u["entries"]
        ])
    logger.info(f"DB: replaced all data from snapshot ({len(created)} users)")


def close_connection() -> None:
    """Close this thread's DB connection (call when a client thread ends)."""
    from django.db import connection
    connection.close()
