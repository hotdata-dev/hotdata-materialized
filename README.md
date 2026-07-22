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

The `@materialize` decorator is coming (see [DESIGN.md](DESIGN.md)); today the
primitives compose in a few lines. The pattern: fingerprint the queryset,
check the registry, read the entry on a hit, run-and-materialize on a miss.

```python
# views.py
import pyarrow as pa
from django.db.models import Count, Sum
from django.http import JsonResponse

from hotdata_materialized import Config, EntryStore, Registry, get_clients
from hotdata_materialized.fingerprint import fingerprint_queryset
from hotdata_materialized.registry import utcnow_iso

from .models import Order


def _cache():
    config = Config.from_django()
    clients = get_clients(config)
    registry = Registry(clients, config)
    return registry, EntryStore(clients, config, registry)


def revenue_by_region(request):
    queryset = (
        Order.objects
        .filter(status="complete")
        .values("region")
        .annotate(orders=Count("id"), revenue=Sum("total"))
        .order_by("-revenue")
    )

    registry, store = _cache()
    fingerprint = fingerprint_queryset(queryset)

    entry = registry.lookup(fingerprint)
    if entry and entry.status == "ready" and not entry.is_expired(utcnow_iso()):
        # HIT: your database is never touched. Small results decode locally
        # from the registry row; large ones stream back as Arrow.
        table = store.read_table(entry)
        return JsonResponse({"rows": table.to_pylist(), "cached": True})

    # MISS: the query runs on your database, exactly as it would uncached.
    rows = list(queryset)
    store.materialize(
        fingerprint,
        pa.Table.from_pylist(rows),
        key="revenue-by-region",   # human-readable label in the registry
        ttl=3600,                  # None = never expires
    )
    # materialize() returned immediately — the parquet upload and registry
    # write are happening in a background thread while you respond.
    return JsonResponse({"rows": rows, "cached": False})
```

Notes on the moving parts:

- **`fingerprint_queryset(qs)`** hashes the compiled `(sql, params)` — no
  cache keys to invent, and two different querysets can't collide. For
  caching a plain function's result use `fingerprint_call(func, args, kwargs,
  version=...)`; bump `version=` to bust the cache on a code change.
- **`materialize()` is write-behind by default**: it returns a pending entry
  immediately and persists in the background. Pass `background=False` to
  block until the entry is ready (e.g. in a management command), and use
  `store.flush()` in tests or scripts to wait for in-flight persists — it
  returns any exceptions they raised.
- **Dataframes:** `store.read_table(entry)` returns a `pyarrow.Table`;
  `.to_pandas()` / `polars.from_arrow(...)` from there.

## Working with a cached entry beyond fetching it

Every entry is its own Hotdata database with the data at `main.data`, so you
can push transformations to Hotdata instead of pulling rows back:

```python
from hotdata.models.query_request import QueryRequest

clients = get_clients(Config.from_django())
top5 = clients.query.query(
    QueryRequest(sql="SELECT region, revenue FROM data ORDER BY revenue DESC LIMIT 5"),
    x_database_id=entry.database_id,
)
```

Refresh or drop an entry explicitly:

```python
store.evict(fingerprint)   # delete the registry row and the entry database
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

First draft. Implemented: fingerprinting, the remote registry, entry store
with write-behind persists, Arrow-native reads, and the TPC-H demo. Next: the
`@materialize` decorator and `MaterializedFrame` handle, stale-while-
revalidate refresh, the sweep command, and vector/BM25 index declarations.

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
