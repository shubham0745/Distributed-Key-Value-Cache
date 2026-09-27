"""
Shared pytest setup.

Tests never touch your real MySQL: before anything imports Django we
point it at a throwaway SQLite file (fresh for every run) and create
the tables. Tests that need real rows use the `clean_db` fixture.
"""
import atexit
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TEST_DB_DIR = tempfile.mkdtemp(prefix="kv-cache-tests-")
os.environ["DB_ENGINE"] = "sqlite"
os.environ["DB_NAME"] = os.path.join(_TEST_DB_DIR, "test.sqlite3")
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings"

import django  # noqa: E402

django.setup()

from django.core.management import call_command  # noqa: E402
from django.db import connections  # noqa: E402

call_command("migrate", verbosity=0)


def _cleanup():
    connections.close_all()
    shutil.rmtree(_TEST_DB_DIR, ignore_errors=True)


atexit.register(_cleanup)

import pytest  # noqa: E402


@pytest.fixture
def clean_db():
    """Empty every table before and after the test."""
    from apps.users.models import CacheUser, CacheEntry
    from apps.cluster.models import RaftMeta, RaftLogEntry

    def wipe():
        CacheEntry.objects.all().delete()
        CacheUser.objects.all().delete()
        RaftLogEntry.objects.all().delete()
        RaftMeta.objects.all().delete()

    wipe()
    yield
    wipe()
