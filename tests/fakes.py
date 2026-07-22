"""Fake Hotdata backend for tests.

Registry SQL executes against a real DuckDB connection per fake managed
database, so statements are validated by an actual engine instead of string
assertions. The databases/uploads fakes record calls and return objects
shaped like the SDK's response models.

Background persists hit these fakes from executor threads, so the mutating
entry points serialize on one lock (DuckDB connections should not be used
concurrently).
"""

from __future__ import annotations

import io
import itertools
import re
import threading
from types import SimpleNamespace

import duckdb
import pyarrow.parquet as pq
from hotdata.exceptions import ApiException


class FakeBackend:
    def __init__(self):
        self._ids = itertools.count(1)
        self._api_lock = threading.RLock()
        self.connections = {}  # database_id -> duckdb connection
        self.database_meta = {}  # database_id -> create request
        self.deleted_databases = []
        self.upload_blobs = {}  # upload_id -> bytes
        self.fail_upload_once = False  # next upload fails, then service recovers

        self.single_loads = []  # (database_id, schema, table, request)
        self.result_tables = {}  # result_id -> pyarrow.Table
        self.arrow_fetches = []  # result_ids fetched via get_result_arrow
        # When set, uploads block until the event fires (concurrency tests).
        self.stall_uploads = None

        # The attributes EntryStore/Registry read off HotdataClients.
        self.query = SimpleNamespace(query=self._query)
        self.results = SimpleNamespace(get_result_arrow=self._get_result_arrow)
        self.databases = SimpleNamespace(
            list_databases=self._list_databases,
            create_database=self._create_database,
            delete_database=self._delete_database,
            load_database_table=self._single_load,
        )
        self.uploads = SimpleNamespace(upload_file=self._upload_file)

    # -- query ---------------------------------------------------------------

    def _query(self, query_request, x_database_id=None, **kwargs):
        with self._api_lock:
            conn = self.connections[x_database_id]
            try:
                cursor = conn.execute(query_request.sql)
            except duckdb.CatalogException as exc:
                # mirror the API's error shape: table 'default.main.<name>' not found
                match = re.search(r"Table with name (\w+)", str(exc))
                name = match.group(1) if match else "unknown"
                raise ApiException(
                    status=400, reason=f"table 'default.main.{name}' not found"
                ) from exc
            if cursor.description is None:
                return SimpleNamespace(
                    columns=[], rows=[], truncated=False, result_id=None
                )
            arrow = cursor.to_arrow_table()
            result_id = f"res_{next(self._ids)}"
            self.result_tables[result_id] = arrow
            return SimpleNamespace(
                columns=arrow.column_names,
                rows=[list(row.values()) for row in arrow.to_pylist()],
                truncated=False,
                result_id=result_id,
            )

    def _get_result_arrow(self, result_id, x_database_id=None, **kwargs):
        with self._api_lock:
            self.arrow_fetches.append(result_id)
            return self.result_tables[result_id]

    # -- databases -------------------------------------------------------------

    def _list_databases(self):
        with self._api_lock:
            return SimpleNamespace(
                databases=[
                    SimpleNamespace(id=db_id, name=req.name)
                    for db_id, req in self.database_meta.items()
                ]
            )

    def _create_database(self, request):
        with self._api_lock:
            db_id = f"db_{next(self._ids)}"
            self.connections[db_id] = duckdb.connect(":memory:")
            self.database_meta[db_id] = request
            return SimpleNamespace(
                id=db_id,
                name=request.name,
                default_catalog="default",
                default_schema="main",
            )

    def _delete_database(self, database_id):
        with self._api_lock:
            if database_id not in self.connections:
                raise ApiException(status=404, reason="database not found")
            del self.connections[database_id]
            del self.database_meta[database_id]
            self.deleted_databases.append(database_id)

    def _declared_key(self, database_id, table_name):
        request = self.database_meta[database_id]
        for schema in request.schemas or []:
            for table in schema.tables or []:
                if table.name == table_name:
                    return table.key
        return None

    def _table_exists(self, conn, name):
        return bool(
            conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                [name],
            ).fetchall()
        )

    def _apply_entry(self, database_id, name, mode, upload_id, key_override=None):
        conn = self.connections[database_id]
        incoming = pq.read_table(io.BytesIO(self.upload_blobs[upload_id]))
        conn.register("incoming", incoming)
        if mode == "replace" or not self._table_exists(conn, name):
            if mode != "delete":
                conn.execute(
                    f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM incoming"
                )
        elif mode in ("upsert", "delete"):
            key_columns = key_override or self._declared_key(database_id, name)
            assert key_columns, f"{mode} load on {name} requires a key"
            match = " AND ".join(
                f"{name}.{col} = incoming.{col}" for col in key_columns
            )
            conn.execute(
                f"DELETE FROM {name} WHERE EXISTS "
                f"(SELECT 1 FROM incoming WHERE {match})"
            )
            if mode == "upsert":
                conn.execute(f"INSERT INTO {name} SELECT * FROM incoming")
        else:
            raise AssertionError(f"unsupported load mode {mode}")
        conn.unregister("incoming")
        if not self._table_exists(conn, name):
            return 0
        return conn.execute(f"SELECT count(*) FROM {name}").fetchone()[0]

    def _single_load(self, database_id, var_schema, table, request):
        with self._api_lock:
            self.single_loads.append((database_id, var_schema, table, request))
            total = self._apply_entry(
                database_id, table, request.mode, request.upload_id, request.key
            )
            return SimpleNamespace(
                row_count=total, schema_name=var_schema, table_name=table
            )

    # -- uploads ---------------------------------------------------------------

    def _upload_file(self, source, *, filename=None, content_type=None, **kwargs):
        # the stall wait stays outside the lock so a stalled upload cannot
        # deadlock the whole fake API
        if self.stall_uploads is not None:
            self.stall_uploads.wait(timeout=10)
        with self._api_lock:
            if self.fail_upload_once:
                self.fail_upload_once = False
                raise ApiException(status=503, reason="storage unavailable")
            upload_id = f"up_{next(self._ids)}"
            self.upload_blobs[upload_id] = bytes(source)
            return SimpleNamespace(upload_id=upload_id, size_bytes=len(source))
