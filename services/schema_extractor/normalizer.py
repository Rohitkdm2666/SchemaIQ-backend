from __future__ import annotations

from typing import Any


def normalize_type(col_type: Any) -> str:
    if col_type is None:
        return "string"

    t = str(col_type).lower()

    if any(x in t for x in ["int", "serial", "bigint", "smallint"]):
        return "integer"
    if any(x in t for x in ["numeric", "decimal", "float", "double", "real"]):
        return "number"
    if any(x in t for x in ["bool"]):
        return "boolean"
    if any(x in t for x in ["date", "time"]):
        return "datetime"

    return "string"
