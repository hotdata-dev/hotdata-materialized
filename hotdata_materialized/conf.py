"""Configuration, loaded from the HOTDATA_MATERIALIZED dict in Django settings.

The Config dataclass is plain and injectable so the library (and its tests)
can run without a configured Django project.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from .exceptions import ConfigurationError

DEFAULT_API_URL = "https://api.hotdata.dev"

_REQUIRED_KEYS = ("API_KEY", "WORKSPACE_ID")


@dataclass(frozen=True)
class Config:
    api_key: str
    workspace_id: str
    api_url: str = DEFAULT_API_URL
    registry_database: str = "materialized_registry"
    inline_threshold_bytes: int = 2 * 1024 * 1024
    entry_prefix: str = "mz_"
    # Server-side entry-database expiry is set to ttl + grace: a safety net
    # behind the sweep, never the primary eviction path.
    entry_expiry_grace_seconds: int = 24 * 3600
    # Write-behind is the default: a miss returns as soon as the caller's data
    # is in hand and the persist runs on a small background pool.
    background: bool = True
    background_max_workers: int = 2

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "Config":
        missing = [key for key in _REQUIRED_KEYS if not raw.get(key)]
        if missing:
            raise ConfigurationError(
                "HOTDATA_MATERIALIZED is missing required keys: " + ", ".join(missing)
            )
        return cls(
            api_key=raw["API_KEY"],
            workspace_id=raw["WORKSPACE_ID"],
            api_url=raw.get("API_URL", DEFAULT_API_URL),
            registry_database=raw.get("REGISTRY_DATABASE", cls.registry_database),
            inline_threshold_bytes=raw.get(
                "INLINE_THRESHOLD_BYTES", cls.inline_threshold_bytes
            ),
            entry_prefix=raw.get("ENTRY_PREFIX", cls.entry_prefix),
            entry_expiry_grace_seconds=raw.get(
                "ENTRY_EXPIRY_GRACE_SECONDS", cls.entry_expiry_grace_seconds
            ),
            background=raw.get("BACKGROUND", cls.background),
            background_max_workers=raw.get(
                "BACKGROUND_MAX_WORKERS", cls.background_max_workers
            ),
        )

    @classmethod
    def from_django(cls) -> "Config":
        from django.conf import settings

        raw = getattr(settings, "HOTDATA_MATERIALIZED", None)
        if not isinstance(raw, dict):
            raise ConfigurationError(
                "Define a HOTDATA_MATERIALIZED dict in Django settings "
                "(required keys: API_KEY, WORKSPACE_ID)."
            )
        return cls.from_dict(raw)
