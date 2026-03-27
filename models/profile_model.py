from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel


class ColumnStatsModel(BaseModel):
    name: str
    null_count: int
    null_percent: float
    distinct_count: Optional[int] = None


class FreshnessModel(BaseModel):
    column: str
    max_value: Optional[str] = None
    age_days: Optional[float] = None


class TableProfileModel(BaseModel):
    name: str
    row_count: int
    columns: List[ColumnStatsModel]
    freshness: Optional[FreshnessModel] = None


class FKOrphanCheckModel(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    orphan_count: int


class ProfileResponse(BaseModel):
    tables: List[TableProfileModel]
    fk_orphans: List[FKOrphanCheckModel] = []
