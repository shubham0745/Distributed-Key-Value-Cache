"""
config/settings.py  (Week 6 — Raft state stored alongside the cache data)

Django is used for two things only: the ORM (persistence) and password
hashing. There are no views, URLs or templates.

Environment variables:
    DB_ENGINE      mysql (default) | sqlite
    DB_NAME        MySQL database name, or SQLite file name/path
    DB_USER, DB_PASSWORD, DB_HOST, DB_PORT   — MySQL only
    DJANGO_SECRET_KEY
    LOG_LEVEL      DEBUG | INFO (default) | WARNING ...

In a cluster every node gets its OWN database (main.py sets DB_NAME from
cluster.json), exactly like every node has its own copy of the Raft log.
"""
import os
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY",
    "change-this-in-production-use-env-variable"
)

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "apps.users",
    "apps.cluster",
]

DB_ENGINE = os.environ.get("DB_ENGINE", "mysql").lower()
DB_NAME = os.environ.get("DB_NAME", "distributed_cache")

if DB_ENGINE == "mysql":
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": DB_NAME,
            "USER": os.environ.get("DB_USER", "root"),
            "PASSWORD": os.environ.get("DB_PASSWORD", "12345"),   # set DB_PASSWORD instead of editing this
            "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
            "PORT": os.environ.get("DB_PORT", "3306"),
            # Our threads live far longer than a web request: reuse a
            # connection for up to 60s, then db_service reconnects.
            "CONN_MAX_AGE": 60,
            "OPTIONS": {
                "charset": "utf8mb4",
            },
        }
    }
elif DB_ENGINE == "sqlite":
    # Handy for trying things out without MySQL, and used by the tests.
    _sqlite_path = Path(DB_NAME if DB_NAME.endswith(".sqlite3") else f"{DB_NAME}.sqlite3")
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": _sqlite_path if _sqlite_path.is_absolute() else BASE_DIR / _sqlite_path,
            "CONN_MAX_AGE": 60,
            "OPTIONS": {"timeout": 20},   # wait for other threads' writes
        }
    }
else:
    raise ImproperlyConfigured(f"DB_ENGINE must be 'mysql' or 'sqlite', got {DB_ENGINE!r}")

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.PBKDF2PasswordHasher",
]

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(asctime)s [%(threadName)s] %(message)s"},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "default"},
    },
    "root": {
        "handlers": ["console"],
        "level": os.environ.get("LOG_LEVEL", "INFO"),
    },
}
