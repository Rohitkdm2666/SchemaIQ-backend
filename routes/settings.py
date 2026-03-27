from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Body

router = APIRouter(tags=["settings"])

SETTINGS_FILE = Path(__file__).resolve().parents[1] / "data" / "settings.json"

DEFAULT_SETTINGS: Dict[str, Any] = {
    "project": {
        "name": "Olist E-Commerce Analysis",
        "description": "Brazilian marketplace schema analysis - Team Kaizen",
    },
    "ai": {
        "selected_model_index": 0,
        "orchestration_framework": "LangGraph (Default)",
    },
    "notifications": {"email": "aditya@kaizen.dev"},
    "toggles": {
        "darkMode": True,
        "autoRefresh": True,
        "showRows": True,
        "compact": False,
        "faiss": True,
        "pinecone": False,
        "retry": True,
        "md": True,
        "csv": True,
        "html": True,
        "json": False,
        "svg": True,
        "png": True,
        "mermaid": False,
        "agentSchema": True,
        "agentRelmap": True,
        "agentProfile": True,
        "agentBiz": True,
        "agentDict": True,
        "agentViz": True,
        "notifyAgent": True,
        "notifyQuality": True,
        "notifySchema": True,
        "notifyError": True,
        "emailWeekly": False,
    },
}


def _deep_merge(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _read_settings_file() -> Dict[str, Any]:
    if not SETTINGS_FILE.exists():
        return {}
    try:
        return json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_settings_file(settings: Dict[str, Any]) -> None:
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_FILE.write_text(json.dumps(settings, indent=2), encoding="utf-8")


@router.get("/settings")
def get_settings():
    stored = _read_settings_file()
    merged = _deep_merge(DEFAULT_SETTINGS, stored)
    return merged


@router.put("/settings")
def save_settings(payload: Dict[str, Any] = Body(default={})):
    current = _deep_merge(DEFAULT_SETTINGS, _read_settings_file())
    updated = _deep_merge(current, payload or {})
    _write_settings_file(updated)
    return {"status": "saved", "settings": updated}
