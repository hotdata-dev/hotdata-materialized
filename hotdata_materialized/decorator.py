"""The @materialize decorator and the MaterializedFrame handle.

Wrap an expensive function with @materialize: a hit returns a frame backed by
Hotdata without calling the function; a miss calls it, returns a frame over
the computed result immediately, and persists write-behind. The wrapped
function must return something table-shaped: a pyarrow.Table, a pandas
DataFrame, or an iterable of dicts (e.g. a Django .values() queryset).

Fail-open: if the hit check or the persist raises a MaterializedError, the
function runs and its result is served uncached — the cache can degrade to
"no cache," never to "no page."
"""

from __future__ import annotations

import functools
import logging
import re
import threading
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pyarrow as pa

from .client import get_clients
from .conf import Config
from .exceptions import MaterializedError
from ._sql import quote_ident, quote_literal
from .fingerprint import fingerprint_call
from .indexes import BM25, Vector
from .registry import STATUS_READY, Registry, RegistryEntry
from .store import DATA_SCHEMA, DATA_TABLE, EntryStore

logger = logging.getLogger(__name__)

_THIS = re.compile(r"\bthis\b")
# quoted chunks ('...' with '' escapes, "..." with "" escapes) pass through
# untouched so a literal like WHERE label = 'this' is never rewritten
_SQL_CHUNKS = re.compile(r"('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")")


def _rewrite_this(sql: str) -> str:
    return "".join(
        part if part.startswith(("'", '"')) else _THIS.sub(DATA_TABLE, part)
        for part in _SQL_CHUNKS.split(sql)
    )


class MaterializedFrame:
    """Handle over a materialized entry. One interface, two backings: a fresh
    miss wraps the locally computed table; a hit reads from Hotdata lazily."""

    def __init__(
        self,
        store: EntryStore,
        entry: RegistryEntry,
        table: Optional[pa.Table] = None,
        *,
        cached: bool,
    ):
        self._store = store
        self.entry = entry
        self._table = table
        self.cached = cached

    def arrow(self) -> pa.Table:
        if self._table is None:
            self._table = self._store.read_table(self.entry)
        return self._table

    def to_pylist(self) -> List[Dict[str, Any]]:
        return self.arrow().to_pylist()

    def df(self) -> Any:
        """The data as a pandas.DataFrame (requires pandas)."""
        return self.arrow().to_pandas()

    def sql(self, sql: str) -> pa.Table:
        """Run SQL server-side against this entry's database; the identifier
        `this` names the cached data (e.g. "SELECT x FROM this LIMIT 5").

        Requires a persisted entry: available on hits, or after the
        write-behind persist lands (a fresh miss raises StoreError until
        then; use background=False on the decorator to persist inline)."""
        return self._store.query_table(self.entry, _rewrite_this(sql))

    def search(self, query: str, *, column: str, limit: int = 10) -> pa.Table:
        """BM25 keyword search over the entry, best matches first. Requires
        a BM25 index on the column (declare with @materialize(index=BM25(...));
        a freshly missed entry may error until the index build finishes)."""
        table_ref = f"default.{DATA_SCHEMA}.{DATA_TABLE}"
        return self._store.query_table(
            self.entry,
            f"SELECT * FROM bm25_search({quote_literal(table_ref)}, "
            f"{quote_literal(quote_ident(column))}, {quote_literal(query)}) "
            f"ORDER BY score DESC LIMIT {int(limit)}",
        )

    def vector_search(self, query: str, *, column: str, limit: int = 10) -> pa.Table:
        """Semantic search over the entry, nearest first. Requires a vector
        index (declare with @materialize(index=Vector(...))). Pass the indexed
        source column; the query text is embedded server-side."""
        return self._store.query_table(
            self.entry,
            f"SELECT *, vector_distance({quote_ident(column)}, "
            f"{quote_literal(query)}) AS distance "
            f"FROM {DATA_TABLE} ORDER BY distance LIMIT {int(limit)}",
        )

    def __len__(self) -> int:
        return self.arrow().num_rows


_runtime_lock = threading.Lock()
_runtime: Optional[Tuple[Registry, EntryStore]] = None


def get_runtime() -> Tuple[Registry, EntryStore]:
    """The process-wide (Registry, EntryStore) pair, built from Django
    settings on first use."""
    global _runtime
    with _runtime_lock:
        if _runtime is None:
            config = Config.from_django()
            clients = get_clients(config)
            registry = Registry(clients, config)
            _runtime = (registry, EntryStore(clients, config, registry))
        return _runtime


def reset_runtime() -> None:
    global _runtime
    with _runtime_lock:
        _runtime = None


def to_arrow(result: Any) -> pa.Table:
    """Normalize a captured result into a pyarrow.Table."""
    if isinstance(result, pa.Table):
        return result
    if type(result).__module__.split(".")[0] == "pandas":
        return pa.Table.from_pandas(result)
    rows = list(result)
    if rows and not isinstance(rows[0], dict):
        raise TypeError(
            "materialize expects a pyarrow.Table, a pandas.DataFrame, or an "
            "iterable of dicts (e.g. a .values() queryset); got an iterable "
            f"of {type(rows[0]).__name__}"
        )
    return pa.Table.from_pylist(rows)


def materialize(
    key: Optional[str] = None,
    *,
    ttl: Optional[int] = 3600,
    version: int = 0,
    background: Optional[bool] = None,
    key_fn: Optional[Callable[..., Any]] = None,
    index: Union[BM25, Vector, Sequence[Union[BM25, Vector]], None] = None,
) -> Callable[[Callable[..., Any]], Callable[..., MaterializedFrame]]:
    """Materialize a function's result into Hotdata.

        @materialize(ttl=3600)
        def revenue_by_region():
            return Order.objects.values("region").annotate(revenue=Sum("total"))

    The call returns a MaterializedFrame either way; `.cached` says which path
    served it. `version=` busts the cache on code changes; `key_fn=` maps
    non-JSON-serializable arguments to a stable identity. `index=` declares
    BM25/Vector search indexes built when the entry persists.
    """
    declarations: Tuple[Union[BM25, Vector], ...]
    if index is None:
        declarations = ()
    elif isinstance(index, (BM25, Vector)):
        declarations = (index,)
    else:
        declarations = tuple(index)
    embedded = any(
        isinstance(d, Vector) and d.provider is not None for d in declarations
    )
    if embedded and len(declarations) > 1:
        # platform constraint: embedding-backed vector indexes cannot coexist
        # with other indexes on the same table
        raise ValueError(
            "an embedding-backed Vector index (provider=...) cannot be "
            "combined with other indexes on the same entry"
        )

    def decorate(func: Callable[..., Any]) -> Callable[..., MaterializedFrame]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> MaterializedFrame:
            registry, store = get_runtime()
            fingerprint = fingerprint_call(
                func, args, kwargs, version=version, key_fn=key_fn
            )
            entry = None
            try:
                entry = registry.lookup(fingerprint)
            except MaterializedError:
                logger.warning(
                    "hit check for %s failed; running uncached",
                    func.__qualname__,
                    exc_info=True,
                )
            if (
                entry is not None
                and entry.status == STATUS_READY
                and not entry.is_expired(store._now())
            ):
                return MaterializedFrame(store, entry, cached=True)

            result = func(*args, **kwargs)
            table = to_arrow(result)
            label = key or func.__qualname__
            try:
                pending = store.materialize(
                    fingerprint,
                    table,
                    key=label,
                    ttl=ttl,
                    version=version,
                    background=background,
                    indexes=declarations,
                )
            except MaterializedError:
                logger.warning(
                    "persist of %s failed; serving the result uncached",
                    label,
                    exc_info=True,
                )
                pending = RegistryEntry(fingerprint=fingerprint, key=label)
            return MaterializedFrame(store, pending, table=table, cached=False)

        return wrapper

    return decorate
