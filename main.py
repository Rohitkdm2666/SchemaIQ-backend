"""
SchemaIQ QueryBot Backend
FastAPI + SQLAlchemy + Gemini API (Text-to-SQL)
Supports dynamically connecting to various database dialects or local files.
"""

import os
import json
import re
import time
import traceback
import pandas as pd
from pathlib import Path
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).parent))
from agent.monitor import monitor
from agent.anomaly_model import detect_table_anomalies
from agent.rules_model import discover_table_rules

# Schema Intelligence Engine Routers
from routes.connection import router as connection_router
from routes.profile import router as profile_router
from routes.preview import router as preview_router
from routes.upload import router as upload_router
from routes.schema import router as schema_router
from routes.twif import router as twif_router
from routes.dictionary import router as dictionary_router
from routes.insights import router as insights_router
from routes.settings import router as settings_router

import google.generativeai as genai
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from db.connection import get_engine as get_active_engine, db_state
load_dotenv(override=True)

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = os.getenv("GEMINI_API_KEY", "")
MODEL   = "gemini-2.5-flash"

if API_KEY:
    genai.configure(api_key=API_KEY)

def get_client():
    if not API_KEY:
        raise HTTPException(status_code=503, detail="GEMINI_API_KEY not set in .env")
    return genai.GenerativeModel(MODEL)

# ── Database Engine ───────────────────────────────────────────────────────────
FALLBACK_DB_PATH = Path(__file__).parent / "olist.db"
# Only use fallback if explicitly requested or if no other URL is found.
# Actually, to make it truly dynamic, we start with None if DATABASE_URL isn't set.
DB_URL = os.environ.get("DATABASE_URL", None)

engine = None
def initialize_engine():
    global engine
    if not DB_URL:
        print("No DATABASE_URL found. Starting in disconnected mode.")
        return

    try:
        if DB_URL.endswith(".csv"):
            engine = create_engine("sqlite:///:memory:")
            df = pd.read_csv(DB_URL)
            tname = Path(DB_URL).stem.lower().replace(" ", "_").replace("-", "_")
            df.to_sql(tname, engine, index=False)
        else:
            engine = create_engine(DB_URL)
        
        db_state.engine = engine
        db_state.db_url = DB_URL
    except Exception as e:
        print(f"Failed to connect to database at {DB_URL}: {e}")

initialize_engine()

def check_db():
    current_engine = db_state.engine
    if current_engine is None:
        raise HTTPException(status_code=503, detail="Database engine not initialized. Check DATABASE_URL.")
    try:
        with current_engine.connect() as conn:
            pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="SchemaIQ Dynamic DB API")

origins = [
    "http://localhost:5173", "http://127.0.0.1:5173",
    "http://localhost:5174", "http://127.0.0.1:5174",
    "https://schemaiqdashboard.netlify.app",
]
frontend_url = os.getenv("FRONTEND_URL", "")
if frontend_url and frontend_url not in origins:
    origins.append(frontend_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# ── Mount Schema Intelligence Routes ──────────────────────────────────────────
app.include_router(connection_router, prefix="/api")
app.include_router(upload_router, prefix="/api")
app.include_router(schema_router, prefix="/api")
app.include_router(profile_router, prefix="/api")
app.include_router(preview_router, prefix="/api")
app.include_router(twif_router, prefix="/api")
app.include_router(dictionary_router, prefix="/api")
app.include_router(insights_router, prefix="/api")
app.include_router(settings_router, prefix="/api")

# ── Dynamic Schema Extraction ─────────────────────────────────────────────────
CACHED_PROMPT = None
CACHED_PROMPT_DB_URL = None

def generate_dynamic_schema_prompt():
    global CACHED_PROMPT, CACHED_PROMPT_DB_URL
    current_db_url = db_state.db_url
    if CACHED_PROMPT and CACHED_PROMPT_DB_URL == current_db_url:
        return CACHED_PROMPT

    check_db()
    current_engine = get_active_engine()
    insp = inspect(current_engine)
    tables = insp.get_table_names()
    dialect = getattr(current_engine, "name", "unknown")
    
    prompt_lines = [
        "You are a SQL expert AI working with a relational database.",
        f"SQL DIALECT: {dialect}",
        "Generate a valid SQL query based on the following dynamic database schema:\n",
        "DATABASE SCHEMA:",
        "---------------"
    ]
    
    for tname in tables:
        prompt_lines.append(f"\n{tname}")
        
        pks = insp.get_pk_constraint(tname).get("constrained_columns", [])
        fks = insp.get_foreign_keys(tname)
        fk_map = {fk["constrained_columns"][0]: f'{fk["referred_table"]}.{fk["referred_columns"][0]}' for fk in fks if fk["constrained_columns"]}
        
        columns = insp.get_columns(tname)
        for c in columns:
            cname = c["name"]
            ctype = str(c["type"])
            tags = []
            if cname in pks: tags.append("PRIMARY KEY")
            if cname in fk_map: tags.append(f"FK→{fk_map[cname]}")
            tag_str = " ".join(tags)
            prompt_lines.append(f"  - {cname:<24} {ctype:<10} {tag_str}")

    prompt_lines.append("\nIMPORTANT RULES:")
    prompt_lines.append("- Output ONLY a valid JSON object")
    prompt_lines.append(f"- Use SQL syntax compatible with {dialect}")
    prompt_lines.append("- LIMIT results to 20 rows unless explicitly requested")
    
    CACHED_PROMPT = "\n".join(prompt_lines)
    CACHED_PROMPT_DB_URL = current_db_url
    return CACHED_PROMPT

def get_sql_system_prompt():
    return generate_dynamic_schema_prompt() + """

Task: Convert the user's natural language question into a working SQL query for this specific dialect.

Return ONLY a valid JSON object — no markdown, no explanation, no code fences:
{
  "sql": "SELECT ...",
  "explanation": "Brief description of what this query does",
  "chart_type": "bar" or "line" or "number" or "table" or null
}

chart_type rules:
- "bar"    → categories, rankings, counts by group
- "line"   → trends over time
- "number" → single aggregate value (COUNT, SUM, AVG)
- "table"  → multi-column results
- null     → when uncertain
"""

FORMAT_SYSTEM_PROMPT = """
You are a friendly data analyst explaining SQL query results.

Return ONLY a valid JSON object — no markdown, no code fences:
{
  "text": "Clear explanation with **bold** for key numbers",
  "insight": "One sentence business insight"
}
"""

def clean_json(raw: str) -> str:
    raw = raw.strip()
    raw = re.sub(r'^```(?:json)?\s*', '', raw)
    raw = re.sub(r'\s*```$', '', raw)
    return raw.strip()

# ── DB Execution ─────────────────────────────────────────────────────────────
def run_query(sql_query: str):
    try:
        current_engine = get_active_engine()
        with current_engine.connect() as conn:
            result = conn.execute(text(sql_query))
            if result.returns_rows:
                columns = list(result.keys())
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                return columns, rows, None
            return [], [], None
    except Exception as e:
        return [], [], str(e)

# ── Gemini helpers ────────────────────────────────────────────────────────────
def ask_gemini_sql(question: str) -> dict:
    client = get_client()
    prompt = get_sql_system_prompt() + f"\n\nQuestion: {question}"
    resp = client.generate_content(prompt)
    raw  = clean_json(resp.text)
    return json.loads(raw)

def ask_gemini_format(question: str, sql_query: str, results: list) -> dict:
    client = get_client()
    prompt = FORMAT_SYSTEM_PROMPT + f"\n\nQuestion: {question}\n\nSQL used:\n{sql_query}\n\nResults:\n{json.dumps(results[:20], indent=2)}"
    resp = client.generate_content(prompt)
    raw  = clean_json(resp.text)
    return json.loads(raw)

def build_chart_data(columns: list, rows: list, chart_type: str):
    if not rows or not columns: return None
    if chart_type in ("bar", "line") and len(columns) > 0:
        label_col = columns[0]
        value_col = columns[1] if len(columns) > 1 else columns[0]
        return [{"name": str(r.get(label_col, ""))[:24], "value": r.get(value_col, 0)} for r in rows[:30]]
    if chart_type == "number":
        return [{"value": list(rows[0].values())[0] if rows else 0}]
    return None

# ── Models ────────────────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    question: str
    history:  list = []

class ChatResponse(BaseModel):
    text:       str
    insight:    Optional[str]  = None
    sql:        Optional[str]  = None
    chart_type: Optional[str]  = None
    chart_data: Optional[list] = None
    columns:    Optional[list] = None
    rows:       Optional[list] = None
    row_count:  Optional[int]  = None
    error:      Optional[str]  = None

class SuggestionsResponse(BaseModel):
    suggestions: list

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "SchemaIQ DB-Agnostic Engine", "db_url": DB_URL.split('@')[-1] if '@' in DB_URL else DB_URL, "ready": engine is not None}

@app.get("/health")
def health():
    db_ok = db_state.engine is not None
    api_ok = bool(API_KEY)
    return {
        "db": db_ok,
        "api_key_set": api_ok,
        "ready": db_ok and api_ok,
    }

CHAT_CACHE = {}

@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    check_db()
    question = req.question.strip()
    if not question: raise HTTPException(status_code=400, detail="Empty question")

    # Check cache
    if question in CHAT_CACHE:
        return CHAT_CACHE[question]

    t0 = time.time()
    sql_query = None
    try:
        sql_resp   = ask_gemini_sql(question)
        sql_query  = sql_resp.get("sql", "").strip()
        chart_type = sql_resp.get("chart_type")

        if not sql_query:
            monitor.record_query(question, None, False, (time.time()-t0)*1000, "No SQL generated")
            return ChatResponse(text="I couldn't generate a SQL query.", error="No SQL generated")

        columns, rows, error = run_query(sql_query)
        if error:
            sql_resp2 = ask_gemini_sql(f"SQL failed: {error}\nFailed SQL:\n{sql_query}\nFix it.")
            sql_query = sql_resp2.get("sql", sql_query).strip()
            chart_type = sql_resp2.get("chart_type", chart_type)
            columns, rows, error = run_query(sql_query)
            if error:
                monitor.record_query(question, sql_query, False, (time.time()-t0)*1000, error)
                return ChatResponse(text=f"SQL error: `{error}`", sql=sql_query, error=error)

        fmt = ask_gemini_format(question, sql_query, rows)
        text = fmt.get("text", "Results compiled.")
        insight = fmt.get("insight")

        chart_data = build_chart_data(columns, rows, chart_type)
        monitor.record_query(question, sql_query, True, (time.time()-t0)*1000)
        
        response = ChatResponse(
            text=text, insight=insight, sql=sql_query,
            chart_type=chart_type, chart_data=chart_data,
            columns=columns, rows=rows[:100], row_count=len(rows)
        )
        
        # Cache successful response
        CHAT_CACHE[question] = response
        return response
    except Exception as e:
        traceback.print_exc()
        monitor.record_query(question, sql_query, False, (time.time()-t0)*1000, str(e))
        return ChatResponse(text="Error processing request.", error=str(e))

@app.get("/schema")
def get_schema():
    check_db()
    insp = inspect(engine)
    tables = []
    relationships = []
    
    for tname in insp.get_table_names():
        cols = insp.get_columns(tname)
        pks = insp.get_pk_constraint(tname).get("constrained_columns", [])
        fks = insp.get_foreign_keys(tname)

        # Build column objects
        schema_cols = []
        for c in cols:
            schema_cols.append({
                "name": c["name"],
                "type": str(c["type"]),
                "notnull": not c.get("nullable", True),
                "pk": c["name"] in pks
            })

        # Build FK mappings
        fk_list = []
        for fk in fks:
            source_col = fk["constrained_columns"][0] if fk["constrained_columns"] else None
            target_col = fk["referred_columns"][0] if fk["referred_columns"] else None
            if source_col and target_col:
                relationships.append({
                    "from_table": tname,
                    "from_col": source_col,
                    "to_table": fk["referred_table"],
                    "to_col": target_col,
                    "cardinality": "N:1",
                })
                fk_list.append({"from_col": source_col, "to_table": fk["referred_table"], "to_col": target_col})

        try:
            with engine.connect() as conn:
                rcnt = conn.execute(text(f'SELECT COUNT(*) FROM "{tname}"')).scalar()
        except:
            rcnt = 0

        tables.append({
            "name": tname,
            "columns": schema_cols,
            "primary_keys": pks,
            "foreign_keys": fk_list,
            "row_count": rcnt
        })

    return {"tables": tables, "relationships": relationships}

@app.get("/api/suggestions", response_model=SuggestionsResponse)
def get_suggestions():
    check_db()
    try:
        client = get_client()
        schema = generate_dynamic_schema_prompt()
        
        prompt = schema + """
        
        Task: Based on the database schema above, suggest 5 highly specific and interesting natural language questions a business user might ask this EXACT database.
        
        Guidelines:
        - Use REAL table and column names from the schema.
        - Vary the complexity (e.g., one simple count, two aggregations/rankings, one time-series trend, one joined query).
        - Focus on business value (revenue, customers, growth, performance).
        
        Example of GOOD, detailed questions:
        - "What are the top 5 product categories by total revenue in 2023?"
        - "Which city has the highest number of repeat customers?"
        - "How has the average order value changed month-over-month?"
        - "Who are the top 10 sellers by delivery performance (average shipping time)?"
        
        Return ONLY a valid JSON object — no markdown, no code fences:
        {
          "suggestions": [
            "Actual question 1...",
            "Actual question 2...",
            ...
          ]
        }
        """
        
        resp = client.generate_content(prompt)
        raw  = clean_json(resp.text)
        data = json.loads(raw)
        
        # If AI returns empty or generic junk, return a better default
        suggestions = data.get("suggestions", [])
        if not suggestions or len(suggestions) < 3:
            raise ValueError("AI returned too few suggestions")
            
        return SuggestionsResponse(suggestions=suggestions)
    except Exception as e:
        print(f"Failed to generate suggestions: {e}")
        traceback.print_exc()
        # Better fallback that is slightly more "active"
        return SuggestionsResponse(suggestions=[
            "Show me a summary of my tables",
            "What are the top 5 records in my most populated table?",
            "How many rows are there in each table?",
            "Show me a distribution of values in a key column",
            "Are there any NULL values in my primary tables?"
        ])

@app.get("/profile")
def get_profile():
    check_db()
    insp = inspect(engine)
    table_profiles = []
    total_completeness = 0
    tcount = 0

    for tname in insp.get_table_names():
        try:
            with engine.connect() as conn:
                row_count = conn.execute(text(f'SELECT COUNT(*) FROM "{tname}"')).scalar()
                
                if not row_count or row_count == 0:
                    table_profiles.append({"table": tname, "row_count": 0, "quality_score": 100.0, "columns": []})
                    continue

                cols = insp.get_columns(tname)
                col_profiles = []
                nulls_t = 0
                
                for c in cols:
                    cname = c["name"]
                    n_cnt = conn.execute(text(f'SELECT COUNT(*) FROM "{tname}" WHERE "{cname}" IS NULL')).scalar() or 0
                    dist = conn.execute(text(f'SELECT COUNT(DISTINCT "{cname}") FROM "{tname}"')).scalar() or 0
                    
                    null_pct = round((n_cnt / row_count) * 100, 2)
                    col_profiles.append({
                        "name": cname, "null_count": n_cnt, "null_pct": null_pct, "distinct_count": dist
                    })
                    nulls_t += n_cnt

                score = round((1 - (nulls_t / (row_count * len(cols)))) * 100, 1)
                total_completeness += score
                
                # Route through custom AI Models
                anomalies = []
                rules = []
                try:
                    anomalies = detect_table_anomalies(engine, tname)
                    rules = discover_table_rules(engine, tname)
                except Exception as ml_err:
                    print(f"ML Models failed for {tname}: {ml_err}")

                table_profiles.append({
                    "table": tname, 
                    "row_count": row_count, 
                    "quality_score": score, 
                    "columns": col_profiles,
                    "anomalies": anomalies,
                    "rules": rules
                })
                tcount += 1
        except Exception as e:
            print(f"Error profiling {tname}: {e}")

    overall = round(total_completeness / tcount, 1) if tcount else 100
    return {"overall_quality": overall, "table_count": tcount, "tables": table_profiles}

@app.get("/metrics")
def get_metrics():
    stats = monitor.get_stats()
    stats["db_exists"] = engine is not None
    stats["api_key_set"] = bool(API_KEY)
    stats["sql_dialect"] = engine.name if engine else "Offline"
    return stats