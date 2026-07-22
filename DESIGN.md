# hotdata-materialized — Design

**Package:** `hotdata-materialized` (module `hotdata_materialized`)
**Primitive:** `@materialize` decorator → `MaterializedFrame` handle
**Status:** Draft v0.2 (2026-07-22)

## What it is

A Django library that offloads expensive database queries and transformations to
HotData. Wrap an expensive function with `@materialize`: on a **miss**, the
function runs in the host application's environment and the result is captured
and loaded into HotData as parquet; on a **hit**, the host app's database is
never touched — the caller gets a `MaterializedFrame` backed by a HotData
managed database that supports dataframes, SQL, and vector/BM25 search.

This is not a fast key-value cache. It is a **capability cache**: a hit does
not save microseconds, it saves the host database from running a
seconds-to-minutes analytical query, and it returns a *queryable dataset*
rather than a blob. The vocabulary is materialized views, not Redis: entries
are snapshots with a refresh lifecycle.

## Non-goals

- Not a `django.core.cache` replacement for sessions, per-request memoization,
  or hot key-value lookups — Redis beats this by orders of magnitude on read
  latency. (A thin `BaseCache` compatibility shim may ship later; it is
  explicitly secondary.)
- Not a Django database backend, router, or monkey-patch of `Model.objects`.
  An earlier prototype explored those approaches and rejected them: backends
  imply full SQL/write support, and patching makes the data source implicit.
- No automatic query interception. Capture is always explicit per call site —
  data leaving the host infrastructure must be opt-in.
- Cross-entry SQL joins are out of scope for v1 (see "Database-per-entry").

## Core decisions

### 1. Package name: `hotdata-materialized`

The `hotdata-<name>` naming convention elsewhere denotes integrations with
third-party tools. The adjective form `-materialized` reads as "materialized
[views/results]" rather than as an integration with a product of that name.
API vocabulary uses the concept directly: `@materialize`,
`MaterializedFrame`.

### 2. Remote lookup via the HotData API (no local state)

The hit/miss check is a query against a **registry** — a small managed
database in HotData (`materialized_registry` by default) holding one row per
entry. The host application needs **no migrations, no local tables, no Redis**.
This is the strongest adoption property for a slip-in shim: `pip install`,
add a settings dict, decorate a function.

Costs and mitigations:

- **Every hit check is one API round trip** (~100ms+ vs sub-ms local). The
  economics still work because the fit is miss-cost ≫ hit-cost, but the
  registry lookup query is designed to do double duty: it returns entry
  metadata *and* the inline payload (see "Small-result short-circuit") in a
  single round trip, so a small chart hit is served with exactly one API call.
- **Registry availability = cache availability.** If the API is unreachable,
  the fail-open policy (below) runs the wrapped function locally. The library
  degrades to "no cache," never to "no page."
- **Cold start:** if the workspace runtime has scaled to zero, the first
  lookup after idle carries spin-up latency. SWR mode mitigates; a
  warm-keepalive ping is a possible later addition.
- An optional in-process micro-memo (per-process dict with a short TTL, e.g.
  5s) can collapse repeated lookups within a burst of requests. Off by
  default; not required for correctness.

Registry schema (one table, `main.entries`, in the registry database):

```sql
CREATE TABLE entries (
    fingerprint     TEXT PRIMARY KEY,   -- content-address of the entry
    key             TEXT,               -- human-readable label from the decorator
    database_id     TEXT,               -- the entry's managed database (db_...)
    status          TEXT,               -- building | ready | failed
    created_at      TIMESTAMP,
    expires_at      TIMESTAMP,
    row_count       BIGINT,
    byte_size       BIGINT,
    inline_payload  TEXT,               -- base64 Arrow IPC, if small
    result_id       TEXT,               -- persisted SELECT * result, minted at
                                        -- persist time; remote hits fetch it as
                                        -- Arrow in one call
    schema_json     TEXT,               -- captured column names/types
    version         INT                 -- decorator version= at write time
);
```

Reads go through `POST /v1/queries` scoped to the registry database
(`X-Database-Id`). **Writes cannot** — the query endpoint is read-only
(DML and DDL are rejected). Registry writes instead use the managed-table
load path: the
entries table is declared with `key=["fingerprint"]` at database-create time,
and every write is a one-row parquet load in `upsert` or `delete` mode. An
empty replace-mode load at bootstrap publishes the schema so SELECTs work
before the first entry; a registry found unseeded (crashed bootstrap)
self-heals the same way on first read.

Loads use the single-table endpoint
(`POST /v1/databases/{id}/schemas/{s}/tables/{t}/loads`), which supports
replace/append/upsert/update/delete with per-load `key`. The batch
endpoint (with `commit_id` idempotency) can be adopted when available — a
TODO marks the call site.

**Idempotency, honestly:** the single-table endpoint has no `commit_id`, so
entry loads are idempotent *by effect* only — a replayed replace-mode load
into the entry's own database converges to the same state, and registry
upserts converge by key. What is NOT covered is retry ambiguity after a
timed-out response. The single-table endpoint also accepts a `result_id` —
a persisted query result can be published as a table's contents
server-side, which step 2 should use to materialize Hotdata-side query
results without round-tripping data through the client.

### 3. Database-per-entry

Every materialized entry is **its own managed database**, named
`mz_<fingerprint32>` (a display label — identity is the server-issued
database id, mapped from the full fingerprint by the registry). The captured result lands in a fixed
location inside it: `main.data`.

What this buys:

- **Eviction is atomic and total:** `DELETE /v1/databases/{id}` drops the
  data, its indexes, and its storage in one call. No orphaned-table sweep
  logic; the sweep is "for each expired registry row, delete one database."
  Evict order is row-first, database-second: a failure mid-evict orphans a
  database for the reconciliation sweep, never a ready row over deleted data.
- **Isolation and scoped auth:** `POST /v1/auth/database` mints short-lived
  database-scoped tokens. A `MaterializedFrame` can carry a token scoped to
  *only its own entry*, which makes it safe to hand frame references to
  less-trusted contexts (e.g., a browser-facing chart proxy) without exposing
  the workspace.
- **Uniform addressing:** every frame is `(database_id, "main", "data")`.
  `.sql()` rewrites the `this` placeholder to `main.data` and sets
  `X-Database-Id` — no cross-entry name collisions possible.
- **Per-entry indexes** (vector/BM25/sorted) attach naturally to the entry's
  lifecycle and die with it.

Costs:

- **Miss overhead:** one extra API call (`POST /v1/databases`) and any
  provisioning latency per miss. Misses are already seconds-scale
  (local query + parquet upload), so this is marginal.
- **Database count growth:** a busy app can create many databases. The sweep
  keeps steady-state bounded at (active entries), but workspace quotas /
  per-database overhead on the HotData side need confirmation. **Open
  question #1.**
- **No cross-entry joins in v1:** a query executes within one managed
  database. Joining two materialized entries would require attaching one to
  the other or materializing a combined entry. Deliberately deferred; if it
  becomes a real demand, `frame.join(other_frame)` can materialize a derived
  entry.

## Lifecycle

### Hit

1. Compute fingerprint (pure local computation).
2. One registry query:
   `SELECT database_id, status, expires_at, inline_payload, schema_json FROM entries WHERE fingerprint = $1`.
3. `status = ready` and not expired → return a `MaterializedFrame`:
   - If `inline_payload` present, the frame is **pre-loaded**: `.df()` /
     `.arrow()` serve from the payload with no further API calls. `.sql()` and
     search still execute remotely against the entry database.
   - Otherwise the frame is **remote-backed** and lazy: nothing else is
     fetched until the caller consumes data.
4. Expired + SWR mode → return the stale frame immediately, trigger a
   background refresh (thread or Celery task re-running the miss path).
   Expired + strict mode → fall through to miss.

### Miss

1. Best-effort lock: `INSERT` a `status='building'` row for the fingerprint
   (primary-key conflict → someone else is building; in SWR mode serve stale
   or, for a cold miss, either wait-and-poll briefly or compute anyway —
   duplicate computation is safe because loads are idempotent).
2. Run the wrapped function in the host environment.
3. Normalize the result to `pyarrow.Table`:
   - Django `QuerySet` → `.values()` rows via `.iterator(chunk_size=…)`,
     streamed into record batches (bounded memory, never a full
     materialization in the web process),
   - pandas / polars / pyarrow / list-of-dicts → direct conversion.
4. `POST /v1/databases` → create `mz_<fingerprint16>`.
5. Write parquet; `UploadsApi.upload_file()` (presigned, direct to object
   storage); `DatabasesApi.batch_load_database_tables()` with
   `commit_id = fingerprint` → `main.data`. Idempotent: crashed or
   concurrent misses cannot double-load.
6. If the decorator declared indexes, `IndexesApi.create_index()` (async job;
   see "Search indexes").
7. `UPDATE` the registry row → `status='ready'`, `expires_at`,
   `inline_payload` if the result is under the inline threshold
   (default 2 MiB Arrow IPC), schema, counts.
8. Return a **locally-backed** frame wrapping the in-memory Arrow table — the
   misser never round-trips to read back data it just computed.

Write mode: **write-behind by default**: the miss
returns the locally-backed frame as soon as the data is captured, and steps
4–7 run on a small in-process `ThreadPoolExecutor`. No Celery required — the
work is network I/O and C-level parquet encoding, both GIL-releasing.
Perceived miss latency ≈ the direct query. Consequences, accepted by design:
until mark_ready lands there is no registry row, so concurrent misses on
other processes duplicate the source query (idempotent, just wasted compute);
a killed worker loses the persist (next request misses again); background
failures are logged loudly, surfaced by `flush()`, and leave no zombie row.
`background=False` (per call or via config) restores synchronous
write-through for callers that need `.sql()` on the entry immediately.

## MaterializedFrame

One interface, three backings (inline-payload, remote, local-after-miss);
calling code never branches.

```python
frame.arrow()                    # pyarrow.Table
frame.df()                       # pandas (via Arrow)
frame.pl()                       # polars
frame.sql("SELECT channel, sum(n) FROM this GROUP BY 1")   # runs on HotData
frame.filter(channel="organic").order_by("-n")[:100]        # chainable facade
frame.search("outage", column="notes")                      # BM25
frame.vector_search("churn risk", column="notes", k=20)     # vector index
frame.refresh()                  # force re-materialization
frame.invalidate()               # delete registry row + entry database
frame.meta                       # fingerprint, database_id, created_at, expires_at, row_count
```

- `.sql()` substitutes the identifier `this` with `main.data` and executes
  with `X-Database-Id: <entry db>`. Large results use the SDK's truncation
  auto-follow / Arrow streaming.
- The chainable facade is an immutable, lazy queryset compiler (Django
  lookup syntax), pointed at the entry table instead of a source model.
- On a locally-backed frame, `.df()/.arrow()` are free; `.sql()` and search
  delegate to the remote entry once `status='ready'` (write-through
  guarantees this immediately; in write-behind mode they block-or-raise until
  the load lands — configurable).

## Decorator API

```python
from hotdata_materialized import materialize, Vector

@materialize(
    key="daily-signups",          # label; fingerprint still includes args
    ttl=3600,                     # seconds; None = manual invalidation only
    version=2,                    # bump to bust on code changes
    mode="swr",                   # "swr" (default) | "strict"
    background=True,              # write-behind (default) | write-through
    index=Vector(column="notes", provider="openai", metric="cosine"),
    invalidate_on=[Event],        # opt-in signal-based staleness marking
    on_error="fallback",          # "fallback" (default) | "raise"
)
def daily_signups(start, end):
    return (Event.objects
            .filter(created_at__range=(start, end))
            .values("date", "channel")
            .annotate(n=Count("id")))
```

Also usable imperatively for one-off QuerySets:

```python
from hotdata_materialized import from_queryset
frame = from_queryset(qs, ttl=900)
```

## Fingerprinting

- **Functions:** SHA-256 over (module-qualified name, `version`, normalized
  args). Args must be JSON-serializable or the decorator must supply
  `key_fn=`; arbitrary-object `repr()` is rejected rather than silently
  producing unstable fingerprints across processes.
- **QuerySets:** hash the `(sql, params)` tuple from
  `qs.query.get_compiler(using).as_sql()` — not `str(qs.query)`, which
  inlines parameters unreliably.
- False misses are safe; false hits are not. Two logically identical queries
  written differently will fingerprint differently — accepted and documented.

## Invalidation and freshness

- **TTL** is primary: checked at lookup time; enforced by the **sweep** (a
  management command / periodic task: select expired registry rows, delete
  each entry database, delete the row). Since eviction is our responsibility
  entirely — nothing server-side expires entry databases — the sweep is
  mandatory operational documentation, not optional polish. A reconciliation
  pass also deletes `mz_*` databases with no live registry row (crash
  leftovers).
- **Signal invalidation** (`invalidate_on=[Model]`) wires
  `post_save`/`post_delete` to mark entries stale. Honest scope: it only sees
  ORM writes in this codebase's processes — raw SQL, other services, and ETL
  are invisible. Opt-in per entry, never global/automatic.
- Entries are **snapshots**. Staleness within TTL is expected behavior, same
  as any materialized view.

## Stampede control

- The `status='building'` registry row (primary-key insert) is the
  cross-server, best-effort lock — no local DB or Redis needed, consistent
  with the no-local-state decision.
- SWR mode (default) makes lock contention mostly irrelevant: expired entries
  serve stale while exactly one builder refreshes.
- Cold-miss races that slip past the lock are safe: database creation is
  keyed by name, loads are keyed by `commit_id`.

## Failure policy

Fail-open by default: any HotData error on lookup or fetch falls back to
executing the wrapped function locally (the miss path always exists) and logs
a warning. `on_error="raise"` for users who want their primary DB protected
from silent re-load. A registry row pointing at a deleted database is treated
as a miss.

## Search indexes

Declared on the decorator (`index=Vector(...)` / `BM25(...)` / `Sorted(...)`),
created right after load. Index builds are **async jobs** on the HotData side:
`.vector_search()` before the job completes raises `IndexNotReadyError`
(with the job id) rather than silently returning empty results;
`wait=True` polls with a timeout. Embedding happens server-side via the
workspace's configured embedding provider.

## Configuration

```python
# settings.py
HOTDATA_MATERIALIZED = {
    "API_URL": "https://api.hotdata.dev",      # default
    "API_KEY": env("HOTDATA_API_KEY"),          # durable hd_... key; SDK handles JWT exchange
    "WORKSPACE_ID": "ws_...",
    "REGISTRY_DATABASE": "materialized_registry",  # created on first use
    "DEFAULT_TTL": 3600,
    "DEFAULT_MODE": "swr",
    "INLINE_THRESHOLD_BYTES": 2 * 1024 * 1024,
    "ON_ERROR": "fallback",
    "ENTRY_PREFIX": "mz_",                      # entry database name prefix
}
```

Built on the Hotdata Python SDK (`hotdata` package with the `arrow`
extra): transparent
JWT exchange, layered retry (connection resets, 429 shedding, result
polling), presigned multipart uploads, Arrow IPC streaming. The SDK is
synchronous — fine for WSGI; async views get `sync_to_async` wrappers
(`await frame.adf()` etc.). Native async is deferred.

## Build order

1. **Core plumbing:** fingerprinting; registry bootstrap (create registry DB +
   `entries` table on first use); entry store (create DB → upload → batch
   load → registry upsert; delete path). Pure SDK composition.
2. **`@materialize` + `MaterializedFrame`** with the three backings,
   `.df()/.arrow()/.pl()/.sql()`, inline-payload short-circuit, fail-open.
3. **Chainable facade:** immutable lazy queryset compiler over the entry
   table (Django lookup syntax).
4. **Freshness machinery:** SWR, building-row lock, sweep management command +
   reconciliation, signal invalidation.
5. **Search:** `Vector`/`BM25` declarations, job polling, `IndexNotReadyError`.
6. **Extras:** write-behind mode, `sync_to_async` wrappers, optional
   `BaseCache` shim, `{% materialized %}` template helper for chart contexts.

## Open questions

1. **Workspace limits on managed database count** — database-per-entry needs
   a sanity check against quotas and any per-database cost/overhead on the
   HotData side.
2. **Registry write throughput** — every persist costs an upload session +
   load; confirm this is comfortable for bursty miss storms.
3. **Multi-environment layout** — one registry per environment
   (dev/staging/prod) via `REGISTRY_DATABASE` naming, or per workspace?
   Leaning: one workspace per environment, registry name fixed.
4. **Cold-start UX** — is a keepalive ping worth it, or is SWR enough?
