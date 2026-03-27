from __future__ import annotations

from typing import Any, Dict, List


def map_foreign_keys(structured_schema: Dict[str, Any]) -> Dict[str, Any]:
    tables = structured_schema.get("tables", [])

    relationships: List[Dict[str, Any]] = []

    for t in tables:
        fks_out: List[Dict[str, Any]] = []
        for fk in t.get("_raw_foreign_keys", []) or []:
            constrained = fk.get("constrained_columns") or []
            referred_table = fk.get("referred_table")
            referred_columns = fk.get("referred_columns") or []

            for idx, col in enumerate(constrained):
                ref_col = referred_columns[idx] if idx < len(referred_columns) else (referred_columns[0] if referred_columns else None)
                if not (referred_table and ref_col):
                    continue

                fks_out.append(
                    {
                        "column": col,
                        "references": {"table": referred_table, "column": ref_col},
                    }
                )

                relationships.append(
                    {
                        "from_table": t.get("name"),
                        "to_table": referred_table,
                        "type": "many-to-one",
                    }
                )

        t["foreign_keys"] = fks_out
        t.pop("_raw_foreign_keys", None)

    # de-dup relationships
    uniq = []
    seen = set()
    for r in relationships:
        key = (r.get("from_table"), r.get("to_table"), r.get("type"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)

    structured_schema["relationships"] = uniq
    return structured_schema
