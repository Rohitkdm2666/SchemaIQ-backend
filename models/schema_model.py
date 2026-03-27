from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class ColumnModel(BaseModel):
    name: str
    type: str


class FKReferenceModel(BaseModel):
    table: str
    column: str


class ForeignKeyModel(BaseModel):
    column: str
    references: FKReferenceModel


class TableModel(BaseModel):
    name: str
    columns: List[ColumnModel]
    primary_key: List[str] = []
    foreign_keys: List[ForeignKeyModel] = []


class RelationshipModel(BaseModel):
    from_table: str
    to_table: str
    type: str


class SchemaResponse(BaseModel):
    tables: List[TableModel]
    relationships: List[RelationshipModel] = []
