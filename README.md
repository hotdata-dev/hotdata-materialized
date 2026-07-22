# hotdata-materialized

Materialize expensive Django query results into [Hotdata](https://hotdata.dev).
On a cache **miss** your code runs in your environment as usual and the result
is captured into Hotdata as parquet — in the background, after your response
has already returned. On a **hit** your database is never touched: the data
comes back as a `pyarrow.Table` you can turn into a dataframe, or query with
SQL server-side.

Think materialized views, not Redis: entries are snapshots of expensive
analytical results, with a TTL lifecycle, each living in its own Hotdata
managed database. The host application needs **no migrations and no Redis** —
entry metadata lives in a small Hotdata managed database (the registry),
created on first use.

Measured against TPC-H on a Neon Postgres (see [demo/](demo/)):

| | Direct on Postgres | Miss (perceived) | Hit |
|---|---|---|---|
| Q1 pricing summary (full scan, 4 rows) | ~1.4 s | ~1.4 s | **~80 ms** |
| Revenue by part (50,000 rows) | ~2.1 s | ~2.0 s | **~260 ms** |

A miss costs the same as not caching (the persist runs write-behind); a hit
is 8–18× faster with zero load on your database.

## Install

```bash
pip install hotdata-materialized
```

```python
# settings.py
HOTDATA_MATERIALIZED = {
    "API_KEY": env("HOTDATA_API_KEY"),      # hd_... key from app.hotdata.dev
    "WORKSPACE_ID": "work...",
    # optional:
    # "API_URL": "https://api.hotdata.dev",
    # "REGISTRY_DATABASE": "materialized_registry",
    # "INLINE_THRESHOLD_BYTES": 2 * 1024 * 1024,
    # "BACKGROUND": True,                   # write-behind persists (default)
}
```

## Using it in a Django view

Wrap the expensive part in `@materialize`; call it from your view:

```python
# reports.py
from django.db.models import Count, Sum
from hotdata_materialized import materialize

from .models import Order


@materialize(ttl=3600)
def revenue_by_region():
    return (
        Order.objects
        .filter(status="complete")
        .values("region")
        .annotate(orders=Count("id"), revenue=Sum("total"))
        .order_by("-revenue")
    )
```

```python
# views.py
from django.http import JsonResponse
from .reports import revenue_by_region


def revenue_view(request):
    frame = revenue_by_region()
    return JsonResponse({"rows": frame.to_pylist(), "cached": frame.cached})
```

On a **hit** the function never runs and your database is never touched. On a
**miss** the function runs exactly as it would uncached, the caller gets the
result immediately, and the persist happens in a background thread. Either
way you get a `MaterializedFrame`:

- `frame.arrow()` — the data as a `pyarrow.Table` (`frame.df()` for pandas,
  `frame.to_pylist()` for dicts, `len(frame)` for the row count)
- `frame.sql("SELECT region, revenue FROM this ORDER BY revenue DESC LIMIT 5")`
  — SQL runs server-side against the cached entry; `this` names the data
- `frame.cached` — which path served it; `frame.entry` — the registry record

The wrapped function can return an iterable of dicts (a `.values()` queryset),
a `pyarrow.Table`, or a pandas DataFrame. Decorator knobs: `ttl=` (seconds,
`None` = never expires), `version=` (bump to bust the cache on code changes),
`key=` (human-readable label), `key_fn=` (stable identity for arguments that
aren't JSON-serializable), `background=False` (block until persisted).
Fail-open by design: if Hotdata is unreachable, the function runs and its
result is served uncached — the cache degrades to "no cache," never to
"no page."

## Evicting entries

The decorator composes public primitives (`fingerprint_call` /
`fingerprint_queryset`, `Registry`, `EntryStore`) that you can use directly —
for example to drop an entry explicitly:

```python
from hotdata_materialized import fingerprint_call
from hotdata_materialized.decorator import get_runtime

registry, store = get_runtime()
store.evict(fingerprint_call(revenue_by_region))
```

Expired entries stop being served immediately (the `is_expired` check) and
their databases carry a server-side expiry backstop; a sweep command that
deletes them proactively is on the roadmap below.

## How it works

One managed database (the **registry**) holds one row per entry: fingerprint,
status, TTL, the entry's database id, and — for small results — the data
itself as an inline Arrow payload, so a chart-sized hit is served in a single
API round trip. Larger results live in a per-entry managed database and are
fetched as an Arrow IPC stream via a result id minted at persist time (no
re-query on hits). Failures are loud and fail toward "no cache": a failed
persist leaves no registry row, and the next request simply misses again.

See [DESIGN.md](DESIGN.md) for the architecture and the accepted
trade-offs.

## Status / roadmap

First draft. Implemented: the `@materialize` decorator and
`MaterializedFrame` (first cut), fingerprinting, the remote registry, entry
store with write-behind persists, Arrow-native reads, and the TPC-H demo.
Next: stale-while-revalidate refresh, the sweep command, a chainable queryset
facade on the frame, and vector/BM25 index declarations.

## Demo

[demo/](demo/) is a small Django project that benchmarks TPC-H queries on a
Neon Postgres directly vs. through the cache:

```bash
cd demo
python manage.py compare                   # Q1: tiny result, inline hit
python manage.py compare --scenario parts  # 50k rows: remote Arrow hit
```

## Development

```bash
uv venv && uv pip install -e '.[test]'
uv run pytest
```
