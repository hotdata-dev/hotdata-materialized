"""Demo settings: TPC-H in Neon as the app database, hotdata-materialized on top.

Configuration comes from the environment:
  NEON_DATABASE_URL     Postgres connection string with the tpch_sf1 schema
  HOTDATA_API_KEY       Hotdata API key (a session JWT also works)
  HOTDATA_WORKSPACE_ID  Workspace public id
"""

import os
from urllib.parse import urlparse

SECRET_KEY = "demo-not-secret"
DEBUG = True
USE_TZ = True

INSTALLED_APPS = ["tpch"]


def _neon_url():
    url = os.environ.get("NEON_DATABASE_URL")
    if url:
        return url
    raise RuntimeError("Set NEON_DATABASE_URL")


_parsed = urlparse(_neon_url())

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _parsed.path.lstrip("/"),
        "USER": _parsed.username,
        "PASSWORD": _parsed.password,
        "HOST": _parsed.hostname,
        "PORT": _parsed.port or 5432,
        "OPTIONS": {
            "options": "-c search_path=tpch_sf1,public",
            "sslmode": "require",
        },
    }
}

HOTDATA_MATERIALIZED = {
    "API_KEY": os.environ.get("HOTDATA_API_KEY", ""),
    "WORKSPACE_ID": os.environ.get("HOTDATA_WORKSPACE_ID", ""),
    "REGISTRY_DATABASE": "materialized_registry_demo",
    "ENTRY_PREFIX": "mzdemo_",
    # low threshold so the parts scenario exercises the remote Arrow read
    # path while q1 stays on the inline-payload path
    "INLINE_THRESHOLD_BYTES": 256 * 1024,
}
