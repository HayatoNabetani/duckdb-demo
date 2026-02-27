"""transition_url_item_statistics 一覧 API（クエリパラメータで絞り込み・keyset ページネーション）"""

import time

from fastapi import APIRouter, Depends, Response
from sqlalchemy.orm import Session

from crud import read_transition_url_item_statistics
from database import get_session
from schemas import (
    TransitionUrlItemStatisticsListResponse,
    TransitionUrlItemStatisticsSearchQuery,
)

router = APIRouter(prefix="/transition_url_item_statistics", tags=["transition_url_item_statistics"])


def get_db():
    yield from get_session()


@router.get("", response_model=TransitionUrlItemStatisticsListResponse)
def list_transition_url_item_statistics(
    response: Response,
    query_params: TransitionUrlItemStatisticsSearchQuery = Depends(),
    db: Session = Depends(get_db),
):
    """
    transition_url_item_statistics を transition_url_id で集約し、
    合計値と cost_difference 最大の代表行（app_id / account 等）を返す。
    limit と next で keyset ページネーション対応。
    レスポンスヘッダー: X-Elapsed-Seconds（全体）, X-Query-Seconds（DuckDB クエリのみ）。
    """
    t0 = time.perf_counter()
    result, query_seconds = read_transition_url_item_statistics(db=db, query_params=query_params)
    elapsed = time.perf_counter() - t0
    response.headers["X-Elapsed-Seconds"] = f"{elapsed:.3f}"
    response.headers["X-Query-Seconds"] = f"{query_seconds:.3f}"
    return result
