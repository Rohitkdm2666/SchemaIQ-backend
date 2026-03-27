from __future__ import annotations

from typing import Optional

from fastapi import APIRouter
from fastapi import HTTPException

from db.connection import get_engine
from db.connection import test_connection
from db.inspector import inspect_database
from models.schema_model import SchemaResponse
from services.relationship_mapper.fk_mapper import map_foreign_keys
from services.relationship_mapper.infer_mapper import infer_relationships
from services.schema_extractor.extractor import extract_schema
from services.schema_formatter.formatter import format_schema

router = APIRouter(tags=["schema"])


@router.get("/schema", response_model=SchemaResponse)
def get_schema(db_url: Optional[str] = None, schema: Optional[str] = None, infer: bool = False):
    try:
        engine = get_engine(db_url=db_url)
        if db_url:
            test_connection(engine)

        raw = inspect_database(engine, schema=schema)
        structured = extract_schema(raw)
        with_fks = map_foreign_keys(structured)

        if infer:
            inferred = infer_relationships(with_fks)
            existing = {(r.get("from_table"), r.get("to_table"), r.get("type")) for r in with_fks.get("relationships", [])}
            for r in inferred:
                key = (r.get("from_table"), r.get("to_table"), r.get("type"))
                if key not in existing:
                    with_fks.setdefault("relationships", []).append(r)

        return format_schema(with_fks)
    except HTTPException:
        return {"tables": [], "relationships": []}
    except Exception as e:
        return {"tables": [], "relationships": []}
