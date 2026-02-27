"""
FastAPI アプリ: R2 から取得した Parquet を DuckDB に載せ、
SQLAlchemy で動的クエリして transition_url_item_statistics 一覧を返す。

起動: uv run uvicorn main_api:app --reload
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from router import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 起動時にデータパスを確保（R2 から取得 or ローカルキャッシュ）
    from data_loader import ensure_data_path
    try:
        ensure_data_path()
    except FileNotFoundError as e:
        print(e)
    yield
    # shutdown
    pass


app = FastAPI(title="Transition URL Item Statistics (DuckDB)", lifespan=lifespan)
app.include_router(router)


@app.get("/health")
def health():
    return {"status": "ok"}
