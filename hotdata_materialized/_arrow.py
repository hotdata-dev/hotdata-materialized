"""Arrow/parquet encoding helpers shared by the registry and the entry store."""

from __future__ import annotations

import base64
import io
import json
from typing import Optional

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq


def table_to_parquet_bytes(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def table_to_ipc_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with pa_ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def encode_inline_payload(table: pa.Table, threshold_bytes: int) -> Optional[str]:
    # threshold bounds the payload as stored (post-base64), not the raw IPC
    payload = base64.b64encode(table_to_ipc_bytes(table)).decode("ascii")
    if len(payload) > threshold_bytes:
        return None
    return payload


def decode_inline_payload(payload: str) -> pa.Table:
    with pa_ipc.open_stream(io.BytesIO(base64.b64decode(payload))) as reader:
        return reader.read_all()


def schema_to_json(table: pa.Table) -> str:
    return json.dumps(
        [{"name": field.name, "type": str(field.type)} for field in table.schema]
    )
