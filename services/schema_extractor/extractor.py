from __future__ import annotations

from typing import Any, Dict, List

from services.schema_extractor.normalizer import normalize_type


def extract_schema(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    tables_out: List[Dict[str, Any]] = []

    for t in raw_data.get("tables", []):
        columns_out = []
        for c in t.get("columns", []):
            col_name = c.get("name")
            col_type = normalize_type(c.get("type"))
            columns_out.append({"name": col_name, "type": col_type})

        tables_out.append(
            {
                "name": t.get("name"),
                "columns": columns_out,
                "primary_key": t.get("primary_key", []) or [],
                "_raw_foreign_keys": t.get("foreign_keys", []) or [],
            }
        )

    return {"tables": tables_out}
