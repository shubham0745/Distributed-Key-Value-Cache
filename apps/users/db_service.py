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
    save_entry()        → upsert a key-value pair (+ optional expiry)
    get_entry()         → read one value (RAM cache miss)
    get_entry_with_expiry() → read one value and its expiry
    delete_entry()      → remove a key-value pair
    set_expiry()        → change / clear a key's expiry
    get_all_entries()   → load entries for a user (newest `limit` if given)
    get_expiries()      → every key of a user that has an expiry
    list_keys()         → a user's live keys
    delete_expired()    → remove every key that has expired
    next_expiry()       → the earliest expiry time, if any
    load_all_users()    → load every user on startup
    load_sessions() / save_session() / delete_session()
                        → the client request table (exactly-once writes)
    dump_to()           → write every row as JSON lines (Raft snapshot)
    replace_from()      → overwrite everything with such a snapshot
    close_connection()  → close this thread's DB connection
"""
import functools
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000      # rows per INSERT when restoring a snapshot


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
def save_entry(username: str, key: str, value: str, expire_at: Optional[int] = None) -> None:
    """
    Upsert (update or insert) a cache entry for a user.

    'upsert' means:
      - if (user, key) row EXISTS → update the value
      - if it DOESN'T exist       → create it

    Django's update_or_create() does this in a single SQL statement.
    This is called every time a SET / SETEX entry is applied. A plain
    SET clears any expiry, like Redis.
    """
    from apps.users.models import CacheUser, CacheEntry
    try:
        user = CacheUser.objects.get(username=username)
        CacheEntry.objects.update_or_create(
            user=user,
            cache_key=key,
            defaults={"cache_value": value, "expire_at": expire_at}
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
def get_entry_with_expiry(username: str, key: str) -> Optional[tuple[str, Optional[int]]]:
    """(value, expire_at) for one key, or None if it isn't stored."""
    from apps.users.models import CacheEntry
    return (CacheEntry.objects
            .filter(user__username=username, cache_key=key)
            .values_list("cache_value", "expire_at")
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
def set_expiry(username: str, key: str, expire_at: Optional[int]) -> bool:
    """Set (or clear, with None) a key's expiry. False if the key isn't stored."""
    from apps.users.models import CacheEntry
    updated = (CacheEntry.objects
               .filter(user__username=username, cache_key=key)
               .update(expire_at=expire_at))
    return updated > 0


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
def get_expiries(username: str) -> dict[str, int]:
    """{key: expire_at} for every key of this user that has an expiry."""
    from apps.users.models import CacheEntry
    return dict(CacheEntry.objects
                .filter(user__username=username, expire_at__isnull=False)
                .values_list("cache_key", "expire_at"))


@_db_call
def list_keys(username: str, now_ms: int) -> list[str]:
    """Every key of this user that hasn't expired at `now_ms`, sorted."""
    from django.db.models import Q
    from apps.users.models import CacheEntry
    return list(CacheEntry.objects
                .filter(user__username=username)
                .filter(Q(expire_at__isnull=True) | Q(expire_at__gt=now_ms))
                .order_by("cache_key")
                .values_list("cache_key", flat=True))


@_db_call
def delete_expired(now_ms: int) -> int:
    """Delete every key (any user) whose expiry is at or before `now_ms`."""
    from apps.users.models import CacheEntry
    deleted, _ = CacheEntry.objects.filter(expire_at__lte=now_ms).delete()
    return deleted


@_db_call
def next_expiry() -> Optional[int]:
    """The earliest expire_at of any key, or None."""
    from django.db.models import Min
    from apps.users.models import CacheEntry
    return CacheEntry.objects.aggregate(first=Min("expire_at"))["first"]


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
def load_sessions() -> list[tuple[str, int, str, int]]:
    """Every (client_id, last_seq, result_json, last_index)."""
    from apps.users.models import ClientSession
    return list(ClientSession.objects.values_list("client_id", "last_seq", "result", "last_index"))


@_db_call
def save_session(client_id: str, seq: int, result_json: str, index: int) -> None:
    from apps.users.models import ClientSession
    ClientSession.objects.update_or_create(
        client_id=client_id,
        defaults={"last_seq": seq, "result": result_json, "last_index": index},
    )


@_db_call
def delete_session(client_id: str) -> None:
    from apps.users.models import ClientSession
    ClientSession.objects.filter(client_id=client_id).delete()


@_db_call
def dump_to(out) -> None:
    """
    Write every user, entry and client session as JSON lines — the Raft
    snapshot of this node. Rows are streamed, so the dataset never has to
    fit in memory:
        {"u": username, "h": password_hash}
        {"e": [username, key, value, expire_at]}
        {"s": [client_id, last_seq, result_json, last_index]}
    """
    from apps.users.models import CacheUser, CacheEntry, ClientSession
    for username, password_hash in CacheUser.objects.order_by("id").values_list(
            "username", "password_hash").iterator(chunk_size=BATCH_SIZE):
        out.write(json.dumps({"u": username, "h": password_hash}) + "\n")
    for row in CacheEntry.objects.order_by("updated_at", "id").values_list(
            "user__username", "cache_key", "cache_value", "expire_at").iterator(chunk_size=BATCH_SIZE):
        out.write(json.dumps({"e": list(row)}) + "\n")
    for row in ClientSession.objects.order_by("id").values_list(
            "client_id", "last_seq", "result", "last_index").iterator(chunk_size=BATCH_SIZE):
        out.write(json.dumps({"s": list(row)}) + "\n")


@_db_call
def replace_from(lines) -> int:
    """
    Overwrite every user, entry and session with a snapshot written by
    dump_to(). All-or-nothing (one transaction), inserted in batches.
    Returns the number of users.
    """
    from django.db import transaction
    from apps.users.models import CacheUser, CacheEntry, ClientSession

    with transaction.atomic():
        CacheEntry.objects.all().delete()
        CacheUser.objects.all().delete()
        ClientSession.objects.all().delete()

        user_ids: dict[str, int] = {}
        users, entries, sessions = [], [], []

        def flush_users():
            CacheUser.objects.bulk_create(users)
            names = [u.username for u in users]
            user_ids.update(CacheUser.objects.filter(username__in=names)
                            .values_list("username", "id"))
            users.clear()

        for line in lines:
            if not line.strip():
                continue
            row = json.loads(line)
            if "u" in row:
                users.append(CacheUser(username=row["u"], password_hash=row["h"]))
                if len(users) >= BATCH_SIZE:
                    flush_users()
            elif "e" in row:
                if users:
                    flush_users()
                username, key, value, expire_at = row["e"]
                entries.append(CacheEntry(user_id=user_ids[username], cache_key=key,
                                          cache_value=value, expire_at=expire_at))
                if len(entries) >= BATCH_SIZE:
                    CacheEntry.objects.bulk_create(entries)
                    entries.clear()
            elif "s" in row:
                client_id, seq, result, index = row["s"]
                sessions.append(ClientSession(client_id=client_id, last_seq=seq,
                                              result=result, last_index=index))
                if len(sessions) >= BATCH_SIZE:
                    ClientSession.objects.bulk_create(sessions)
                    sessions.clear()
        if users:
            flush_users()
        CacheEntry.objects.bulk_create(entries)
        ClientSession.objects.bulk_create(sessions)
    logger.info(f"DB: replaced all data from snapshot ({len(user_ids)} users)")
    return len(user_ids)


def close_connection() -> None:
    """Close this thread's DB connection (call when a client thread ends)."""
    from django.db import connection
    connection.close()
