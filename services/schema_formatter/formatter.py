from __future__ import annotations

from typing import Any, Dict

from models.schema_model import SchemaResponse


def format_schema(schema_payload: Dict[str, Any]) -> Dict[str, Any]:
    model = SchemaResponse.model_validate(schema_payload)
    return model.model_dump()
