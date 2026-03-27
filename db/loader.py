from __future__ import annotations

import csv
import datetime as _dt
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _safe_table_name(file_stem: str) -> str:
    name = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in file_stem)
    name = name.strip("_") or "data"
    if name[0].isdigit():
        name = f"t_{name}"
    return name.lower()


def _sqlite_url(sqlite_path: Path) -> str:
    return f"sqlite+pysqlite:///{sqlite_path.as_posix()}"


def _infer_sqlite_type(values: list[str]) -> str:
    cleaned = [v.strip() for v in values if v is not None and str(v).strip() != ""]
    if not cleaned:
        return "TEXT"

    def _is_int(x: str) -> bool:
        try:
            int(x)
            return True
        except Exception:
            return False

    def _is_float(x: str) -> bool:
        try:
            float(x)
            return True
        except Exception:
            return False

    def _is_date(x: str) -> bool:
        try:
            _dt.date.fromisoformat(x)
            return True
        except Exception:
            return False

    def _is_datetime(x: str) -> bool:
        try:
            _dt.datetime.fromisoformat(x)
            return True
        except Exception:
            return False

    if all(_is_int(v) for v in cleaned):
        return "INTEGER"
    if all(_is_float(v) for v in cleaned):
        return "REAL"
    if all(_is_datetime(v) for v in cleaned):
        return "DATETIME"
    if all(_is_date(v) for v in cleaned):
        return "DATE"

    return "TEXT"


def build_sqlite_from_csv(
    csv_path: Path,
    sqlite_path: Path,
    table_name: Optional[str] = None,
    *,
    append: bool = True,
    sample_rows: int = 50,
) -> str:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if table_name is None:
        table_name = _safe_table_name(csv_path.stem)

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV file is empty")

        columns = [h.strip() or "column" for h in header]
        columns = ["".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in c) for c in columns]
        columns = [c if c else f"column_{i+1}" for i, c in enumerate(columns)]

        if sqlite_path.exists() and not append:
            sqlite_path.unlink()

        # sample values for type inference
        sample_matrix: list[list[str]] = []
        for _ in range(sample_rows):
            try:
                sample_matrix.append(next(reader))
            except StopIteration:
                break

        # reset reader by re-opening
        f.seek(0)
        reader = csv.reader(f)
        next(reader)

        col_samples: list[list[str]] = [[] for _ in columns]
        for row in sample_matrix:
            if len(row) < len(columns):
                row = row + [""] * (len(columns) - len(row))
            elif len(row) > len(columns):
                row = row[: len(columns)]
            for i, v in enumerate(row):
                col_samples[i].append(v)

        col_types = [_infer_sqlite_type(vals) for vals in col_samples]

        conn = sqlite3.connect(str(sqlite_path))
        try:
            cur = conn.cursor()
            col_defs = ", ".join([f'"{c}" {t}' for c, t in zip(columns, col_types)])
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs});')

            placeholders = ",".join(["?"] * len(columns))
            quoted_columns = ", ".join([f'"{column}"' for column in columns])
            insert_sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders});'

            for row in reader:
                if len(row) < len(columns):
                    row = row + [None] * (len(columns) - len(row))
                elif len(row) > len(columns):
                    row = row[: len(columns)]
                cur.execute(insert_sql, row)

            conn.commit()
        finally:
            conn.close()

    return _sqlite_url(sqlite_path)


def build_sqlite_from_sql(sql_path: Path, sqlite_path: Path) -> str:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
        except OSError:
            pass
    sql_text = sql_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(str(sqlite_path))
    try:
        conn.executescript(sql_text)
        conn.commit()
    finally:
        conn.close()

    return _sqlite_url(sqlite_path)


def load_file(file_path: str, sqlite_out_path: Optional[str] = None, *, append: bool = True) -> Dict[str, Any]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    suffix = path.suffix.lower()
    if suffix not in {".csv", ".sql"}:
        raise ValueError("Only .csv or .sql files are supported")

    sqlite_path = Path(sqlite_out_path) if sqlite_out_path else (path.parent / f"{path.stem}.sqlite")

    if suffix == ".csv":
        db_url = build_sqlite_from_csv(path, sqlite_path, append=append)
    else:
        db_url = build_sqlite_from_sql(path, sqlite_path)

    engine: Engine = create_engine(db_url)
    return {"path": str(path), "type": suffix.lstrip("."), "db_url": db_url, "sqlite_path": str(sqlite_path), "engine": engine}

