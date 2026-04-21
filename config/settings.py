"""
Minimal Django settings.
Right now we only use Django for:
  1. Password hashing (make_password / check_password)
  2. MySQL models for users + Raft log (Week 3)

We do NOT use Django's HTTP layer, views, URLs, or templates.
Django here is purely a database + utilities toolkit.
"""

SECRET_KEY = "change-this-in-production-use-env-variable"

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "apps.users",       # Our custom user app (Week 3)
]

# ── Database (MySQL) ──────────────────────────────────────
# Week 2: not used yet — password hashing works without DB
# Week 3: we'll activate this and run migrations
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "distributed_cache",
        "USER": "root",
        "PASSWORD": "123",          # change to your MySQL password
        "HOST": "127.0.0.1",
        "PORT": "3306",
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ── Password hashing ──────────────────────────────────────
# Django uses PBKDF2 by default — strong and secure
PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.PBKDF2PasswordHasher",
]