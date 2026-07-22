"""The remote registry: one managed database holding one row per entry.

The hit/miss check is a SELECT against this database — the host application
keeps no local state. POST /v1/queries is read-only (DML and DDL are
rejected), so registry WRITES go through the managed-table load path instead:
the entries table is declared with key=["fingerprint"] at create time, and
every write is a one-row parquet load in `upsert` or `delete` mode. Values in
read-side WHERE clauses are encoded with _sql.quote_literal because the query
endpoint has no bind parameters.
"""

from __future__ import annotations

import datetime as _dt
import threading
from dataclasses import asdict, dataclass
from typing import Any, List, Optional

import pyarrow as pa
from hotdata.models.create_database_request import CreateDatabaseRequest
from hotdata.models.database_default_schema_decl import DatabaseDefaultSchemaDecl
from hotdata.models.database_default_table_decl import DatabaseDefaultTableDecl
from hotdata.models.load_managed_table_request import LoadManagedTableRequest
from hotdata.models.query_request import QueryRequest

from ._arrow import table_to_parquet_bytes
from ._sql import quote_literal
from .conf import Config
from .exceptions import RegistryError

REGISTRY_SCHEMA = "main"
ENTRIES_TABLE = "entries"

# Timestamps are ISO-8601 UTC strings ("YYYY-MM-DDTHH:MM:SS+00:00"): for a
# fixed format, lexicographic order equals chronological order, so expiry
# comparisons work as plain string comparisons in SQL.
ENTRIES_ARROW_SCHEMA = pa.schema(
    [
        ("fingerprint", pa.string()),
        ("key", pa.string()),
        ("database_id", pa.string()),
        ("status", pa.string()),
        ("created_at", pa.string()),
        ("expires_at", pa.string()),
        ("row_count", pa.int64()),
        ("byte_size", pa.int64()),
        ("inline_payload", pa.string()),
        ("result_id", pa.string()),
        ("schema_json", pa.string()),
        ("version", pa.int64()),
    ]
)

_COLUMNS = ENTRIES_ARROW_SCHEMA.names

STATUS_BUILDING = "building"
STATUS_READY = "ready"
STATUS_FAILED = "failed"


def utcnow_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()


def _iso_plus_seconds(iso: str, seconds: int) -> str:
    moment = _dt.datetime.fromisoformat(iso)
    return (moment + _dt.timedelta(seconds=seconds)).isoformat()


@dataclass
class RegistryEntry:
    fingerprint: str
    key: Optional[str] = None
    database_id: Optional[str] = None
    status: str = STATUS_BUILDING
    created_at: str = ""
    expires_at: Optional[str] = None
    row_count: Optional[int] = None
    byte_size: Optional[int] = None
    inline_payload: Optional[str] = None
    # Persisted result of SELECT * on the entry, minted at persist time:
    # remote hits fetch it as Arrow directly instead of re-running the query.
    result_id: Optional[str] = None
    schema_json: Optional[str] = None
    version: int = 0

    def is_expired(self, now_iso: str) -> bool:
        return self.expires_at is not None and self.expires_at <= now_iso

    @classmethod
    def from_row(cls, columns: List[str], row: List[Any]) -> "RegistryEntry":
        data = dict(zip(columns, row))
        values = {name: data.get(name) for name in _COLUMNS}
        values["version"] = values["version"] or 0
        return cls(**values)  # type: ignore[arg-type]


class Registry:
    def __init__(self, clients, config: Config, now_fn=utcnow_iso):
        self._clients = clients
        self._config = config
        self._now = now_fn
        self._database_id: Optional[str] = None
        self._bootstrap_lock = threading.Lock()

    # -- bootstrap ---------------------------------------------------------

    def database_id(self) -> str:
        with self._bootstrap_lock:
            if self._database_id is None:
                self._database_id = self._find_or_create_database()
            return self._database_id

    def _find_or_create_database(self) -> str:
        listing = self._clients.databases.list_databases()
        for database in listing.databases:
            if database.name == self._config.registry_database:
                return database.id
        created = self._clients.databases.create_database(
            CreateDatabaseRequest(
                name=self._config.registry_database,
                schemas=[
                    DatabaseDefaultSchemaDecl(
                        name=REGISTRY_SCHEMA,
                        tables=[
                            DatabaseDefaultTableDecl(
                                name=ENTRIES_TABLE, key=["fingerprint"]
                            )
                        ],
                    )
                ],
            )
        )
        # An empty replace-mode load publishes the entries schema so SELECTs
        # work before the first real write.
        self._apply_load(created.id, "replace", ENTRIES_ARROW_SCHEMA.empty_table())
        return created.id

    # -- plumbing ------------------------------------------------------------

    def _execute(self, sql: str, _retried: bool = False):
        try:
            return self._clients.query.query(
                QueryRequest(sql=sql), x_database_id=self.database_id()
            )
        except Exception as exc:
            # A registry database found by name may be declared but never
            # seeded (e.g. a crashed bootstrap); publish the schema and retry.
            if not _retried and f"{ENTRIES_TABLE}' not found" in str(exc):
                self._apply_load(
                    self.database_id(), "replace", ENTRIES_ARROW_SCHEMA.empty_table()
                )
                return self._execute(sql, _retried=True)
            raise RegistryError(f"registry query failed: {exc}") from exc

    def _apply_load(self, database_id: str, mode: str, table: pa.Table) -> None:
        try:
            upload = self._clients.uploads.upload_file(
                table_to_parquet_bytes(table),
                filename=f"registry-{mode}.parquet",
                content_type="application/vnd.apache.parquet",
            )
            self._clients.databases.load_database_table(
                database_id,
                REGISTRY_SCHEMA,
                ENTRIES_TABLE,
                LoadManagedTableRequest(
                    mode=mode,
                    upload_id=upload.upload_id,
                    format="parquet",
                    key=["fingerprint"],
                ),
            )
        except Exception as exc:
            raise RegistryError(f"registry {mode} write failed: {exc}") from exc

    def _upsert(self, entry: RegistryEntry) -> None:
        table = pa.Table.from_pylist([asdict(entry)], schema=ENTRIES_ARROW_SCHEMA)
        self._apply_load(self.database_id(), "upsert", table)

    # -- reads ---------------------------------------------------------------

    def lookup(self, fingerprint: str) -> Optional[RegistryEntry]:
        response = self._execute(
            f"SELECT {', '.join(_COLUMNS)} FROM {ENTRIES_TABLE} "
            f"WHERE fingerprint = {quote_literal(fingerprint)}"
        )
        if not response.rows:
            return None
        return RegistryEntry.from_row(response.columns, response.rows[0])

    def list_expired(self) -> List[RegistryEntry]:
        now = self._now()
        response = self._execute(
            f"SELECT {', '.join(_COLUMNS)} FROM {ENTRIES_TABLE} "
            f"WHERE expires_at IS NOT NULL AND expires_at <= {quote_literal(now)}"
        )
        return [RegistryEntry.from_row(response.columns, row) for row in response.rows]

    # -- writes --------------------------------------------------------------

    def mark_ready(
        self,
        fingerprint: str,
        *,
        database_id: str,
        ttl: Optional[int],
        row_count: int,
        byte_size: int,
        inline_payload: Optional[str] = None,
        result_id: Optional[str] = None,
        schema_json: Optional[str] = None,
        key: Optional[str] = None,
        version: int = 0,
    ) -> RegistryEntry:
        now = self._now()
        entry = RegistryEntry(
            fingerprint=fingerprint,
            key=key,
            database_id=database_id,
            status=STATUS_READY,
            created_at=now,
            expires_at=_iso_plus_seconds(now, ttl) if ttl is not None else None,
            row_count=row_count,
            byte_size=byte_size,
            inline_payload=inline_payload,
            result_id=result_id,
            schema_json=schema_json,
            version=version,
        )
        self._upsert(entry)
        return entry

    def mark_failed(self, fingerprint: str) -> None:
        existing = self.lookup(fingerprint)
        if existing is None:
            return
        existing.status = STATUS_FAILED
        self._upsert(existing)

    def delete(self, fingerprint: str) -> None:
        table = pa.Table.from_pylist(
            [{"fingerprint": fingerprint}],
            schema=pa.schema([("fingerprint", pa.string())]),
        )
        self._apply_load(self.database_id(), "delete", table)
