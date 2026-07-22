"""SQL literal encoding for registry statements.

POST /v1/queries takes SQL text only — there are no bind parameters — so
every value written into registry SQL must go through quote_literal().
"""

from __future__ import annotations

import datetime as _dt
import math

from .exceptions import RegistryError


def quote_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise RegistryError(f"cannot encode non-finite float: {value!r}")
        return repr(value)
    if isinstance(value, (_dt.datetime, _dt.date)):
        return quote_literal(value.isoformat())
    if isinstance(value, str):
        if "\x00" in value:
            raise RegistryError("cannot encode string containing NUL byte")
        return "'" + value.replace("'", "''") + "'"
    raise RegistryError(f"cannot encode SQL literal of type {type(value).__name__}")
