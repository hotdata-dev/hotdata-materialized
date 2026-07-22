"""Entry fingerprinting.

A fingerprint content-addresses one materialized entry. False misses are
safe; false hits are not — so canonicalization rejects anything it cannot
serialize deterministically instead of falling back to repr(), whose output
is process-dependent for arbitrary objects.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import hashlib
import json
import uuid

from .exceptions import FingerprintError

SHORT_LENGTH = 16


def _stable_default(value):
    # Type-tagged so date(2026, 7, 22) and the string "2026-07-22" (etc.)
    # cannot fingerprint identically — a false hit serves someone else's data.
    if isinstance(value, _dt.datetime):
        return f"datetime:{value.isoformat()}"
    if isinstance(value, _dt.date):
        return f"date:{value.isoformat()}"
    if isinstance(value, _dt.time):
        return f"time:{value.isoformat()}"
    if isinstance(value, decimal.Decimal):
        return f"decimal:{value}"
    if isinstance(value, uuid.UUID):
        return f"uuid:{value}"
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    if isinstance(value, (bytes, bytearray)):
        return f"bytes:{value.hex()}"
    raise FingerprintError(
        f"cannot fingerprint value of type {type(value).__name__}; "
        "pass JSON-serializable arguments or supply key_fn= on the decorator"
    )


def canonical_json(obj) -> str:
    try:
        return json.dumps(
            obj, sort_keys=True, separators=(",", ":"), default=_stable_default
        )
    except TypeError as exc:
        raise FingerprintError(str(exc)) from exc


def _digest(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fingerprint_call(func, args=(), kwargs=None, *, version=0, key_fn=None) -> str:
    ref = f"{func.__module__}.{func.__qualname__}"
    if key_fn is not None:
        call = key_fn(*args, **(kwargs or {}))
    else:
        call = {"args": list(args), "kwargs": kwargs or {}}
    return _digest(canonical_json({"fn": ref, "version": version, "call": call}))


def fingerprint_queryset(queryset, *, version=0) -> str:
    # (sql, params) from the compiler, not str(queryset.query) — the latter
    # inlines parameters unreliably and is not stable across backends.
    sql, params = queryset.query.get_compiler(queryset.db).as_sql()
    return _digest(
        canonical_json({"sql": sql, "params": list(params), "version": version})
    )


def short(fingerprint: str) -> str:
    return fingerprint[:SHORT_LENGTH]
