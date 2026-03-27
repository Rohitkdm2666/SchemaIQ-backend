from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError


@dataclass
class DBState:
    db_url: Optional[str] = None
    engine: Optional[Engine] = None
    source: Optional[str] = None # 'database' or 'file'
    display_name: Optional[str] = None


db_state = DBState()


def test_connection(engine: Engine) -> None:
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
    except (OperationalError, DBAPIError) as e:
        raise RuntimeError(f"Database connection test failed: {e}")
    except SQLAlchemyError as e:
        raise RuntimeError(f"Database connection test failed: {e}")


def set_db_url(db_url: str, *, validate: bool = False, source: str = 'database', display_name: Optional[str] = None) -> Engine:
    try:
        engine = create_engine(db_url)
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            f"Missing DB driver dependency for db_url='{db_url}'. Original error: {e}"
        )

    if validate:
        test_connection(engine)

    db_state.db_url = db_url
    db_state.engine = engine
    db_state.source = source
    db_state.display_name = display_name
    return engine


def get_engine(db_url: Optional[str] = None) -> Engine:
    if db_url:
        return set_db_url(db_url)

    if db_state.engine is None:
        raise ValueError("Database is not connected. Provide db_url or call /connect first.")

    return db_state.engine
