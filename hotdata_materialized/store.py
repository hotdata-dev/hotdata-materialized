"""The entry store: one managed database per materialized entry.

Persist path: create database -> upload parquet (presigned, direct to object
storage) -> load into main.data -> mark the registry row ready. Everything
from database creation through registry publication is one failure domain: if
any step fails, the entry database is deleted so a populated-but-unpublished
database cannot leak. Eviction deletes the registry row first, then the
database — a failure mid-evict leaves an orphaned database for the
reconciliation sweep, never a ready row pointing at deleted data.

Write-behind is the default: materialize() returns a pending entry as soon as
the data is captured and runs the persist on a small process-global thread
pool. Failures are logged loudly and surfaced by flush(); they leave no ready
registry row — the next request simply misses again. Until mark_ready lands
there is no registry row, so concurrent misses on other processes may
duplicate the source query (accepted for now).
"""

from __future__ import annotations

import datetime as _dt
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor, wait as futures_wait
from typing import Dict, List, Optional, Set

import pyarrow as pa
from hotdata.arrow import ResultNotReadyError
from hotdata.exceptions import ApiException
from hotdata.models.create_database_request import CreateDatabaseRequest
from hotdata.models.database_default_schema_decl import DatabaseDefaultSchemaDecl
from hotdata.models.database_default_table_decl import DatabaseDefaultTableDecl
from hotdata.models.load_managed_table_request import LoadManagedTableRequest
from hotdata.models.query_request import QueryRequest

from . import fingerprint as fp
from ._arrow import (
    decode_inline_payload,
    encode_inline_payload,
    schema_to_json,
    table_to_parquet_bytes,
)
from .conf import Config
from .exceptions import StoreError
from .registry import STATUS_BUILDING, Registry, RegistryEntry, utcnow_iso

__all__ = [
    "EntryStore",
    "DATA_SCHEMA",
    "DATA_TABLE",
    "decode_inline_payload",
    "encode_inline_payload",
    "schema_to_json",
    "table_to_parquet_bytes",
]

logger = logging.getLogger(__name__)

DATA_SCHEMA = "main"
DATA_TABLE = "data"

_executor_lock = threading.Lock()
_executor: Optional[ThreadPoolExecutor] = None


def _get_executor(max_workers: int) -> ThreadPoolExecutor:
    """Process-global persist pool, sized by the first caller's config."""
    global _executor
    with _executor_lock:
        if _executor is None:
            _executor = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="hotdata-materialized"
            )
        return _executor


class EntryStore:
    def __init__(self, clients, config: Config, registry: Registry, now_fn=utcnow_iso):
        self._clients = clients
        self._config = config
        self._registry = registry
        self._now = now_fn
        self._inflight: Dict[str, Set["Future[None]"]] = {}
        self._inflight_lock = threading.Lock()

    def materialize(
        self,
        fingerprint: str,
        table: pa.Table,
        *,
        key: Optional[str] = None,
        ttl: Optional[int] = None,
        version: int = 0,
        background: Optional[bool] = None,
    ) -> RegistryEntry:
        """Persist a captured result as a materialized entry.

        In background mode (the config default) this returns a pending entry
        (status='building', no database_id) immediately; call flush() to wait
        for in-flight persists. Synchronous mode returns the ready entry."""
        if background is None:
            background = self._config.background
        if not background:
            return self._persist(fingerprint, table, key=key, ttl=ttl, version=version)

        executor = _get_executor(self._config.background_max_workers)
        with self._inflight_lock:
            future = executor.submit(
                self._persist_background, fingerprint, table, key, ttl, version
            )
            self._inflight.setdefault(fingerprint, set()).add(future)

        # The done-callback (not the job body) untracks the future: it fires
        # even for a future that completed before this line, and discarding a
        # set member means an old job can never untrack a newer one.
        def _untrack(done: "Future[None]", fingerprint: str = fingerprint) -> None:
            with self._inflight_lock:
                bucket = self._inflight.get(fingerprint)
                if bucket is not None:
                    bucket.discard(done)
                    if not bucket:
                        del self._inflight[fingerprint]

        future.add_done_callback(_untrack)
        return RegistryEntry(
            fingerprint=fingerprint,
            key=key,
            status=STATUS_BUILDING,
            created_at=self._now(),
            version=version,
        )

    def flush(self, timeout: Optional[float] = None) -> List[BaseException]:
        """Wait for in-flight background persists; return their failures."""
        with self._inflight_lock:
            pending = [f for bucket in self._inflight.values() for f in bucket]
        done, _ = futures_wait(pending, timeout=timeout)
        return [e for e in (f.exception() for f in done) if e is not None]

    def _persist_background(self, fingerprint, table, key, ttl, version) -> None:
        try:
            self._persist(fingerprint, table, key=key, ttl=ttl, version=version)
        except Exception:
            # Loud by design: a quietly failing write-behind path means the
            # cache never fills and nobody notices.
            logger.exception(
                "background persist of entry %s failed; next request will miss",
                fp.short(fingerprint),
            )
            raise

    def _persist(
        self,
        fingerprint: str,
        table: pa.Table,
        *,
        key: Optional[str] = None,
        ttl: Optional[int] = None,
        version: int = 0,
    ) -> RegistryEntry:
        parquet_bytes = table_to_parquet_bytes(table)
        database_id = self._create_entry_database(fingerprint, ttl)
        try:
            upload = self._clients.uploads.upload_file(
                parquet_bytes,
                filename=f"{fp.short(fingerprint)}.parquet",
                content_type="application/vnd.apache.parquet",
            )
            # TODO: adopt the batch loads endpoint (commit_id idempotency)
            # when available; single-table loads have no commit_id.
            load = self._clients.databases.load_database_table(
                database_id,
                DATA_SCHEMA,
                DATA_TABLE,
                LoadManagedTableRequest(
                    mode="replace", upload_id=upload.upload_id, format="parquet"
                ),
            )
            inline_payload = encode_inline_payload(
                table, self._config.inline_threshold_bytes
            )
            # For entries too big to inline, mint a persisted SELECT * result
            # now (usually off the request path): remote hits then fetch it as
            # Arrow in one call instead of re-running the query every read.
            result_id = None
            if inline_payload is None:
                result_id = self._clients.query.query(
                    QueryRequest(sql=f"SELECT * FROM {DATA_TABLE}"),
                    x_database_id=database_id,
                    auto_follow=False,
                ).result_id
            return self._registry.mark_ready(
                fingerprint,
                database_id=database_id,
                ttl=ttl,
                row_count=load.row_count,
                byte_size=len(parquet_bytes),
                inline_payload=inline_payload,
                result_id=result_id,
                schema_json=schema_to_json(table),
                key=key,
                version=version,
            )
        except Exception as exc:
            # One failure domain through registry publication: a populated
            # database without a ready row must not outlive the attempt.
            self._cleanup_failed_build(fingerprint, database_id)
            raise StoreError(f"materializing entry failed: {exc}") from exc

    def read_table(self, entry: RegistryEntry) -> pa.Table:
        """Materialize an entry's data as a pyarrow.Table.

        Inline-payload entries decode locally with no network call. Remote
        entries fetch the persisted SELECT * result minted at persist time as
        an Arrow IPC stream (one call once ready); if that result is gone
        (expired server-side), fall back to re-running the query."""
        if entry.inline_payload:
            return decode_inline_payload(entry.inline_payload)
        database_id = entry.database_id
        if database_id is None:
            raise StoreError(
                f"entry {fp.short(entry.fingerprint)} has no database yet "
                "(still building?)"
            )
        if entry.result_id:
            try:
                return self._fetch_arrow(entry.result_id, database_id)
            except Exception:
                logger.info(
                    "stored result for entry %s no longer readable; re-querying",
                    fp.short(entry.fingerprint),
                )
        try:
            response = self._clients.query.query(
                QueryRequest(sql=f"SELECT * FROM {DATA_TABLE}"),
                x_database_id=database_id,
                auto_follow=False,
            )
            if response.result_id is None:
                # answered fully inline and never persisted; nothing to fetch.
                # Keep the column names even for zero rows.
                if not response.rows:
                    return pa.table({column: [] for column in response.columns})
                return pa.Table.from_pylist(
                    [dict(zip(response.columns, row)) for row in response.rows]
                )
            return self._fetch_arrow(response.result_id, database_id)
        except Exception as exc:
            raise StoreError(
                f"reading entry {fp.short(entry.fingerprint)} failed: {exc}"
            ) from exc

    def _fetch_arrow(self, result_id: str, database_id: str) -> pa.Table:
        """Fetch a persisted result as Arrow, waiting out async persistence
        (the SDK polls status-only with limit=0) when it isn't ready yet."""
        try:
            return self._clients.results.get_result_arrow(result_id, database_id)
        except ResultNotReadyError:
            self._clients.query.wait_for_result(
                result_id, database_id, results_api=self._clients.results
            )
            return self._clients.results.get_result_arrow(result_id, database_id)

    def evict(self, fingerprint: str) -> None:
        # A persist racing an evict would recreate the row after the delete;
        # wait out every in-flight background persist of this entry first.
        with self._inflight_lock:
            pending = list(self._inflight.get(fingerprint, ()))
        if pending:
            futures_wait(pending)
        entry = self._registry.lookup(fingerprint)
        # Row first: a failure after this point orphans a database (the
        # reconciliation sweep's job), never a ready row over deleted data.
        self._registry.delete(fingerprint)
        if entry is not None and entry.database_id:
            self._delete_database(entry.database_id)

    # -- internals -----------------------------------------------------------

    def _create_entry_database(self, fingerprint: str, ttl: Optional[int]) -> str:
        # Server-side expiry (ttl + grace) is a best-effort backstop behind
        # the sweep; label is display-only — identity is the id via registry.
        expires_at = None
        if ttl is not None:
            moment = _dt.datetime.fromisoformat(self._now()) + _dt.timedelta(
                seconds=ttl + self._config.entry_expiry_grace_seconds
            )
            expires_at = moment.isoformat()
        try:
            created = self._clients.databases.create_database(
                CreateDatabaseRequest(
                    name=f"{self._config.entry_prefix}{fingerprint[:32]}",
                    expires_at=expires_at,
                    schemas=[
                        DatabaseDefaultSchemaDecl(
                            name=DATA_SCHEMA,
                            tables=[DatabaseDefaultTableDecl(name=DATA_TABLE)],
                        )
                    ],
                )
            )
        except Exception as exc:
            raise StoreError(f"creating entry database failed: {exc}") from exc
        return created.id

    def _delete_database(self, database_id: str) -> None:
        try:
            self._clients.databases.delete_database(database_id)
        except ApiException as exc:
            if exc.status != 404:
                raise StoreError(
                    f"deleting entry database {database_id} failed: {exc}"
                ) from exc

    def _cleanup_failed_build(self, fingerprint: str, database_id: str) -> None:
        try:
            self._delete_database(database_id)
        except Exception:
            logger.warning(
                "cleanup after failed build of %s left a database behind",
                fp.short(fingerprint),
                exc_info=True,
            )
        try:
            self._registry.mark_failed(fingerprint)
        except Exception:
            logger.warning(
                "could not mark entry %s failed", fp.short(fingerprint), exc_info=True
            )
