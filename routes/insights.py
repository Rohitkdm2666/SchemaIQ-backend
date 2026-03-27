from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import google.generativeai as genai
from fastapi import APIRouter, Query, HTTPException

from db.connection import get_engine
from db.inspector import inspect_database
from services.relationship_mapper.fk_mapper import map_foreign_keys
from services.relationship_mapper.infer_mapper import infer_relationships
from services.schema_extractor.extractor import extract_schema
from services.data_profiler.profiler import profile_database

router = APIRouter(tags=["insights"])


def _get_gemini_client():
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        return None
    genai.configure(api_key=api_key)
    return genai.GenerativeModel("gemini-2.5-flash")


def _parse_json_text(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    clean = text.strip()
    if clean.startswith("```"):
        clean = clean.strip("`")
        if clean.lower().startswith("json"):
            clean = clean[4:].strip()
    try:
        return json.loads(clean)
    except Exception:
        pass
    start = clean.find("{")
    end = clean.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(clean[start : end + 1])
        except Exception:
            return None
    return None


def _fallback_insights(tables: List[Dict[str, Any]], relationships: List[Dict[str, Any]], profile: Dict[str, Any], reason: str) -> Dict[str, Any]:
    table_names = [t.get("name") for t in tables if t.get("name")]
    rel_text = ", ".join(
        f"{r.get('from_table')} → {r.get('to_table')}" for r in relationships[:5] if r.get("from_table") and r.get("to_table")
    )
    if not rel_text:
        rel_text = "No explicit foreign key relationships detected."

    niche = []
    # ... previous logic for niche detection stays same, but maybe limit count
    for t in profile.get("tables", [])[:5]:
        tname = t.get("name")
        for c in (t.get("columns") or []):
            cname = (c.get("name") or "").lower()
            if any(k in cname for k in ["flag", "status", "type", "source", "channel", "code", "reason"]):
                niche.append(
                    {
                        "table": tname,
                        "column": c.get("name"),
                        "why_niche": "High-entropy categorical data.",
                        "use_case": "Drill-down optimization.",
                    }
                )
            if len(niche) >= 4:
                break
        if len(niche) >= 4:
            break

    return {
        "overview_text": f"Schema mapped for {len(table_names)} tables using standard heuristics. Fallback reason: {reason}",
        "table_relationships_text": rel_text,
        "niche_columns": niche,
        "alternate_methods": [
            "Materialize daily summaries.",
            "Use stratified sampling.",
            "Build entity-centric marts.",
        ],
    }


INSIGHTS_CACHE = {}

@router.get("/insights")
def get_insights(
    db_url: Optional[str] = Query(None, description="Database URL"),
    schema: Optional[str] = Query(None, description="Database schema"),
    refresh: bool = Query(False, description="Force refresh cache"),
):
    try:
        from db.connection import db_state
        current_db_url = db_url or db_state.db_url
        cache_key = f"{current_db_url}_{schema}"
        
        if not refresh and cache_key in INSIGHTS_CACHE:
            return INSIGHTS_CACHE[cache_key]


        engine = get_engine(db_url=db_url)

        raw = inspect_database(engine, schema=schema)
        # ... rest of the gathering logic ...
        structured = extract_schema(raw)
        schema_with_fks = map_foreign_keys(structured)
        relationships = schema_with_fks.get("relationships", []) or []

        inferred = infer_relationships(schema_with_fks)
        rel_keys = {(r.get("from_table"), r.get("to_table"), r.get("type")) for r in relationships}
        for r in inferred:
            key = (r.get("from_table"), r.get("to_table"), r.get("type"))
            if key not in rel_keys:
                relationships.append(r)
                rel_keys.add(key)

        profile = profile_database(engine, schema_with_fks, compute_distinct=True)

        context = {
            "tables": [
                {
                    "name": t.get("name"),
                    "columns": [c.get("name") for c in (t.get("columns") or [])],
                    "row_count": next((p.get("row_count") for p in profile.get("tables", []) if p.get("name") == t.get("name")), 0),
                }
                for t in (schema_with_fks.get("tables") or [])
            ],
            "relationships": relationships,
            "profile": profile,
        }

        model = _get_gemini_client()
        if not model:
            insights = _fallback_insights(
                schema_with_fks.get("tables") or [],
                relationships,
                profile,
                "Model unavailable",
            )
            res = {"generated_at": datetime.now().isoformat(), "source": "system", **insights}
            INSIGHTS_CACHE[cache_key] = res
            return res

        prompt = f"""
Analyze the database metadata and produce balanced architectural insights.
Your output must be purely technical and precise. No conversational filler.

Return ONLY valid JSON with this shape:
{{
  "overview_text": "A perfect summary of 2-3 sentences (approx. 180-220 characters) describing the schema's architectural purpose.",
  "table_relationships_text": "A brief overview (max 150 characters) of the primary data flows and cardinality.",
  "niche_columns": [
    {{
      "table": "table_name",
      "column": "column_name",
      "why_niche": "Technical reasoning (max 60 characters).",
      "use_case": "Practical strategy (max 60 characters)."
    }}
  ],
  "alternate_methods": [
    "Method 1 (max 50 chars)",
    "Method 2 (max 50 chars)",
    "Method 3 (max 50 chars)"
  ]
}}

DATABASE CONTEXT:
{json.dumps(context, ensure_ascii=True)}
"""

        resp = model.generate_content(prompt)
        parsed = _parse_json_text(getattr(resp, "text", ""))
        if not parsed:
            parsed = _fallback_insights(
                schema_with_fks.get("tables") or [],
                relationships,
                profile,
                "Parsing failed",
            )

        res = {"generated_at": datetime.now().isoformat(), "source": "system", **parsed}
        INSIGHTS_CACHE[cache_key] = res
        return res
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "generated_at": datetime.now().isoformat(),
            "source": "system",
            **_fallback_insights([], [], {}, f"AI Error: {str(e)}"),
        }


