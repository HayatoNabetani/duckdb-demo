"""
DuckDB 接続とテーブル読み込み。
Parquet またはローカル DuckDB キャッシュから transition_url_item_statistics を用意する。
"""

import time
from pathlib import Path
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from config import get_duckdb_path
from data_loader import ensure_data_path


def _ensure_table_loaded(duckdb_path: Path, parquet_path: Path) -> None:
    """DuckDB ファイルにテーブルが無い／古い場合は Parquet から投入する。"""
    import duckdb

    need_load = not duckdb_path.exists() or (
        parquet_path.exists() and parquet_path.stat().st_mtime > duckdb_path.stat().st_mtime
    )
    if not need_load:
        return
    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute("DROP TABLE IF EXISTS transition_url_item_statistics")
        path_sql = str(parquet_path).replace("'", "''")
        con.execute(
            f"CREATE TABLE transition_url_item_statistics AS SELECT * FROM read_parquet('{path_sql}')"
        )
    finally:
        con.close()


def get_engine() -> Engine:
    """データソースを確保し、DuckDB を用意してから SQLAlchemy Engine を返す。"""
    parquet_path = ensure_data_path()
    duckdb_path = get_duckdb_path()
    t0 = time.perf_counter()
    _ensure_table_loaded(duckdb_path, parquet_path)
    elapsed = time.perf_counter() - t0
    if elapsed > 0.01:
        print(f"[database] テーブル準備: {elapsed:.2f} 秒")
    url = f"duckdb:///{duckdb_path}"
    return create_engine(url, pool_pre_ping=False)


_SessionLocal: Optional[sessionmaker] = None


def get_session_factory() -> sessionmaker:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    return _SessionLocal


def get_session() -> Generator[Session, None, None]:
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        session.close()
