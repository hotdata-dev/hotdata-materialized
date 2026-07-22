"""Search-index declarations for @materialize(index=...).

Indexes are created right after the entry's data loads; builds run as
background jobs on the Hotdata side, so a freshly missed entry may briefly
error on search until its index is ready.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BM25:
    """Keyword (BM25) index over a text column; query via frame.search()."""

    column: str


@dataclass(frozen=True)
class Vector:
    """Vector index; query via frame.vector_search().

    With `provider` set (an embedding provider id), the column is treated
    as text and embedded server-side; vector_search then targets the source
    column and query text is embedded automatically. An embedding-backed
    index cannot share a table with other indexes."""

    column: str
    provider: Optional[str] = None
    metric: Optional[str] = None  # "l2" | "cosine" | "dot"
    dimensions: Optional[int] = None
