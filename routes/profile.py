from __future__ import annotations

from typing import Optional

from fastapi import APIRouter

from db.connection import get_engine
from db.inspector import inspect_database
from models.profile_model import ProfileResponse
from services.data_profiler.profiler import profile_database
from services.relationship_mapper.fk_mapper import map_foreign_keys
from services.schema_extractor.extractor import extract_schema

router = APIRouter(tags=["profile"])


@router.get("/profile", response_model=ProfileResponse)
def get_profile(
    db_url: Optional[str] = None,
    schema: Optional[str] = None,
    compute_distinct: bool = True,
    include_fk_orphans: bool = True,
):
    try:
        engine = get_engine(db_url=db_url)

        raw = inspect_database(engine, schema=schema)
        structured = extract_schema(raw)
        if include_fk_orphans:
            structured = map_foreign_keys(structured)
        else:
            for t in structured.get("tables", []) or []:
                t["foreign_keys"] = []

        payload = profile_database(engine, structured, compute_distinct=compute_distinct)
        if not include_fk_orphans:
            payload["fk_orphans"] = []

        return payload
    except Exception as e:
        # Return a safe empty profile instead of failing the frontend quality dashboards.
        print(f"[profile] Profiling failed: {e}")
        return {"tables": [], "fk_orphans": []}
