# Demo: TPC-H on Neon vs hotdata-materialized

Times a heavy TPC-H aggregation three ways:

1. **Neon direct** — the query runs on your Postgres every time.
2. **Materialized miss** — the query runs on Neon once, and the result is
   captured into a Hotdata entry database as parquet.
3. **Materialized hit** — the result comes back from Hotdata (inline payload
   for small results, entry database for large ones); Neon is never touched.
   Plus a server-side SQL transform over the cached entry.

## Setup

```bash
cd hotdata-materialized
uv venv && uv pip install -e '.[test,demo]'

export HOTDATA_API_KEY=hd_...            # a session JWT also works
export HOTDATA_WORKSPACE_ID=work...
export NEON_DATABASE_URL=postgres://...  # must contain the tpch_sf1 schema
```

## Run

```bash
cd demo
../.venv/bin/python manage.py compare                    # TPC-H Q1: tiny result, inline hit
../.venv/bin/python manage.py compare --scenario parts   # ~200k rows: entry-database hit
../.venv/bin/python manage.py compare --keep             # reuse the existing entry (pure hit path)
```

`compare` evicts and rebuilds the entry each run unless `--keep` is passed.
Entries land in the `materialized_registry_demo` registry with a 1h TTL, in
databases labeled `mzdemo_<fingerprint>`.
