from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


def infer_relationships(structured_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    tables = structured_schema.get("tables", [])

    table_to_cols: Dict[str, Set[str]] = {
        t.get("name"): {c.get("name") for c in (t.get("columns") or []) if c.get("name")} for t in tables
    }

    # reverse index: column_name -> tables that contain it
    col_to_tables: Dict[str, Set[str]] = {}
    for tname, cols in table_to_cols.items():
        if not tname:
            continue
        for col in cols:
            col_to_tables.setdefault(col, set()).add(tname)

    relationships: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str, str]] = set()

    for t in tables:
        tname = t.get("name")
        if not tname:
            continue

        for c in t.get("columns", []) or []:
            cname = c.get("name")
            if not cname or not cname.endswith("_id"):
                continue

            base = cname[: -len("_id")]
            candidates = [base, f"{base}s", f"{base}es"]

            # 1) prefer direct table-name match heuristics (customers, customer, etc.)
            preferred_targets: List[str] = [cand for cand in candidates if cand in table_to_cols]

            # 2) also allow matching by presence of the same id column in another table (works for test_customers)
            column_targets = sorted(list((col_to_tables.get(cname) or set()) - {tname}))

            # keep deterministic ordering: preferred first, then column-based
            targets: List[str] = []
            for tt in preferred_targets + column_targets:
                if tt not in targets:
                    targets.append(tt)

            for target_table in targets:
                if cname not in table_to_cols.get(target_table, set()):
                    continue

                key = (tname, target_table, "many-to-one")
                if key in seen:
                    continue
                seen.add(key)
                relationships.append({"from_table": tname, "to_table": target_table, "type": "many-to-one"})

    return relationships
