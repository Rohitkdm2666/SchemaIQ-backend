from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import inspect
from sqlalchemy.engine import Engine


def inspect_database(engine: Engine, schema: Optional[str] = None) -> Dict[str, Any]:
    inspector = inspect(engine)

    table_names = inspector.get_table_names(schema=schema)

    tables: List[Dict[str, Any]] = []
    for table_name in table_names:
        columns = inspector.get_columns(table_name, schema=schema)
        pk = inspector.get_pk_constraint(table_name, schema=schema) or {}
        fks = inspector.get_foreign_keys(table_name, schema=schema) or []

        tables.append(
            {
                "name": table_name,
                "columns": columns,
                "primary_key": pk.get("constrained_columns") or [],
                "foreign_keys": fks,
            }
        )

    return {"tables": tables}
