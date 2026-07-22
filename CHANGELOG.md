# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-22

- `@materialize` decorator and `MaterializedFrame`: write-behind caching of
  expensive Django query results into Hotdata; hits return Arrow-backed
  frames (`.arrow()/.df()/.to_pylist()`), `.sql()` runs server-side against
  the cached entry
- BM25 and vector search on cached entries: `@materialize(index=BM25(...))` /
  `Vector(..., provider=...)` declarations, `frame.search()` and
  `frame.vector_search()`
- Content-addressed fingerprinting for querysets and function calls
- Remote registry (no migrations or local state in the host app) and
  one-managed-database-per-entry storage with TTL expiry backstop
- TPC-H demo project benchmarking direct Postgres vs the cache

