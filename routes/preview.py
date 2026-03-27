from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from db.connection import get_engine
from db.inspector import inspect_database

router = APIRouter(tags=["preview"])


def _qident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


@router.get("/preview")
def preview_table(
    table: str,
    limit: int = 10,
    db_url: Optional[str] = None,
    schema: Optional[str] = None,
) -> Dict[str, Any]:
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    if limit > 100:
        raise HTTPException(status_code=400, detail="limit must be <= 100")

    engine = get_engine(db_url=db_url)

    raw = inspect_database(engine, schema=schema)
    table_names = {t.get("name") for t in (raw.get("tables") or [])}
    if table not in table_names:
        raise HTTPException(status_code=404, detail=f"Table not found: {table}")

    table_ident = _qident(table)

    # Use a very simple LIMIT query for preview. For DBs that don't support LIMIT,
    # this can be extended later (e.g., MSSQL TOP).
    sql = f"SELECT * FROM {table_ident} LIMIT :limit"

    try:
        with engine.connect() as conn:
            res = conn.exec_driver_sql(sql, {"limit": limit})
            cols = list(res.keys())
            rows = [dict(zip(cols, r)) for r in res.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"table": table, "limit": limit, "rows": rows}
