from __future__ import annotations

from fastapi import APIRouter
from fastapi import HTTPException
from pydantic import BaseModel

from db.connection import set_db_url

router = APIRouter(tags=["connection"])


class ConnectRequest(BaseModel):
    db_url: str


@router.post("/connect")
def connect(req: ConnectRequest):
    try:
        set_db_url(req.db_url, validate=True)
        return {"status": "connected"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/connection")
def get_connection():
    from db.connection import db_state
    if not db_state.db_url:
        return {"status": "disconnected", "url": None, "engine": None}
    
    engine = db_state.engine
    engine_name = getattr(engine, "name", "unknown") if engine else "unknown"
    return {
        "status": "connected",
        "url": db_state.db_url,
        "engine": engine_name,
        "source": db_state.source,
        "display_name": db_state.display_name
    }
