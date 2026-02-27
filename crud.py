"""
transition_url_item_statistics を transition_url_id で集約し、
SQLAlchemy で動的にクエリを組み立てて取得する（keyset ページネーション対応）。
"""

import base64
import json
import time
from datetime import datetime
from typing import Optional, Tuple

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from schemas import (
    TransitionUrlItemStatisticsItem,
    TransitionUrlItemStatisticsListResponse,
    TransitionUrlItemStatisticsSearchQuery,
)

# テーブルは DuckDB に存在するので、reflection で読み込む
def _get_table(engine):
    from sqlalchemy import MetaData, Table
    metadata = MetaData()
    return Table("transition_url_item_statistics", metadata, autoload_with=engine)


def _decode_cursor(next_token: Optional[str], sort_val: str) -> Optional[tuple]:
    if not next_token:
        return None
    try:
        raw = base64.b64decode(next_token).decode()
        data = json.loads(raw)
        return (data.get("v"), data.get("id"))
    except Exception:
        return None


def _encode_cursor(v, tid: int) -> str:
    return base64.b64encode(json.dumps({"v": v, "id": tid}).encode()).decode()


def _row_to_item(row: dict) -> TransitionUrlItemStatisticsItem:
    return TransitionUrlItemStatisticsItem(**{k: row.get(k) for k in TransitionUrlItemStatisticsItem.model_fields})


def read_transition_url_item_statistics(
    db: Session,
    query_params: TransitionUrlItemStatisticsSearchQuery,
) -> Tuple[TransitionUrlItemStatisticsListResponse, float]:
    """集約＋代表行結合＋フィルタ＋ソート＋keyset ページネーション。"""
    engine = db.get_bind()
    M = _get_table(engine)

    limit = query_params.limit
    fetch_limit = limit + 1
    sort_val = (query_params.sort or "cost_difference-desc").strip().lower()
    cursor = _decode_cursor(query_params.next, sort_val)

    # 集計用カラム（interval は固定で _2 を使用）
    cost_diff_col = getattr(M.c, "cost_difference_2", M.c.cost)  # カラム名が無ければ fallback
    play_count_diff_col = getattr(M.c, "play_count_difference_2", M.c.play_count)
    max_play_diff_col = getattr(M.c, "max_play_count_difference_2", M.c.max_play_count)
    max_cost_diff_col = getattr(M.c, "max_cost_difference_2", M.c.max_cost)
    cost_count_col = getattr(M.c, "cost_difference_count_2", func.coalesce(M.c.cost, 0) * 0)

    conditions = []
    if query_params.app_id is not None:
        ids = [int(x) for x in query_params.app_id.split(",")]
        conditions.append(M.c.app_id == ids[0] if len(ids) == 1 else M.c.app_id.in_(ids))
    if query_params.is_affiliate is not None:
        conditions.append(M.c.is_affiliate == query_params.is_affiliate)
    if query_params.media_type is not None:
        conditions.append(M.c.media_type == query_params.media_type)
    if query_params.product_id is not None:
        ids = [int(x) for x in query_params.product_id.split(",")]
        conditions.append(M.c.product_id == ids[0] if len(ids) == 1 else M.c.product_id.in_(ids))
    if query_params.genre_id is not None:
        ids = [int(x) for x in query_params.genre_id.split(",")]
        conditions.append(M.c.genre_id == ids[0] if len(ids) == 1 else M.c.genre_id.in_(ids))
    if query_params.transition_type_id is not None:
        ids = [int(x) for x in query_params.transition_type_id.split(",")]
        conditions.append(
            M.c.transition_type_id == ids[0] if len(ids) == 1 else M.c.transition_type_id.in_(ids)
        )

    # 代表行: transition_url_id ごとに cost_difference_2 最大の1行
    rep_subq = select(
        M.c.transition_url_id,
        M.c.thumbnail_url,
        M.c.app_id,
        M.c.is_affiliate,
        M.c.media_type,
        M.c.product_id,
        M.c.genre_id,
        M.c.product_name,
        M.c.genre_name,
        M.c.account_name,
        M.c.account_url,
        M.c.account_icon_url,
        M.c.creation_time,
        M.c.streaming_periods,
        func.row_number().over(partition_by=M.c.transition_url_id, order_by=cost_diff_col.desc()).label("rn"),
    )
    if conditions:
        rep_subq = rep_subq.where(*conditions)
    rep_subq = rep_subq.subquery("rep_subq")
    rep_filtered = (
        select(rep_subq.c.transition_url_id, rep_subq.c.thumbnail_url, rep_subq.c.app_id, rep_subq.c.is_affiliate,
               rep_subq.c.media_type, rep_subq.c.product_id, rep_subq.c.genre_id, rep_subq.c.product_name,
               rep_subq.c.genre_name, rep_subq.c.account_name, rep_subq.c.account_url, rep_subq.c.account_icon_url,
               rep_subq.c.creation_time, rep_subq.c.streaming_periods)
        .where(rep_subq.c.rn == 1)
        .subquery("rep_filtered")
    )

    # 集約
    agg_stmt = select(
        M.c.transition_url_id,
        M.c.transition_url,
        M.c.transition_type_id,
        M.c.transition_type,
        M.c.aggregation_time,
        func.sum(M.c.play_count).label("play_count"),
        func.sum(M.c.cost).label("cost"),
        func.max(M.c.max_play_count).label("max_play_count"),
        func.max(M.c.max_cost).label("max_cost"),
        func.sum(play_count_diff_col).label("play_count_difference"),
        func.sum(cost_diff_col).label("cost_difference"),
        func.max(max_play_diff_col).label("max_play_count_difference"),
        func.max(max_cost_diff_col).label("max_cost_difference"),
        func.sum(cost_count_col).label("cost_difference_count"),
        func.sum(case((M.c.media_type == "video", 1), else_=0)).label("video_count"),
        func.sum(case((M.c.media_type == "banner", 1), else_=0)).label("banner_count"),
        func.sum(case((M.c.media_type == "carousel", 1), else_=0)).label("carousel_count"),
    )
    if conditions:
        agg_stmt = agg_stmt.where(*conditions)
    agg_stmt = agg_stmt.group_by(
        M.c.transition_url_id, M.c.transition_url, M.c.transition_type_id, M.c.transition_type, M.c.aggregation_time
    ).subquery("agg_subq")

    # 集約 + 代表行 結合
    final_stmt = select(
        agg_stmt.c.aggregation_time,
        rep_filtered.c.app_id,
        rep_filtered.c.is_affiliate,
        rep_filtered.c.product_id,
        rep_filtered.c.genre_id,
        rep_filtered.c.product_name,
        rep_filtered.c.genre_name,
        rep_filtered.c.thumbnail_url,
        agg_stmt.c.transition_url_id,
        agg_stmt.c.transition_url,
        rep_filtered.c.creation_time,
        rep_filtered.c.streaming_periods,
        agg_stmt.c.transition_type_id,
        agg_stmt.c.transition_type,
        rep_filtered.c.account_name,
        rep_filtered.c.account_url,
        rep_filtered.c.account_icon_url,
        agg_stmt.c.play_count,
        agg_stmt.c.cost,
        agg_stmt.c.max_play_count,
        agg_stmt.c.max_cost,
        agg_stmt.c.play_count_difference,
        agg_stmt.c.cost_difference,
        agg_stmt.c.max_play_count_difference,
        agg_stmt.c.max_cost_difference,
        agg_stmt.c.cost_difference_count,
        agg_stmt.c.video_count,
        agg_stmt.c.banner_count,
        agg_stmt.c.carousel_count,
    ).select_from(
        agg_stmt.join(rep_filtered, agg_stmt.c.transition_url_id == rep_filtered.c.transition_url_id)
    )

    # カーソル条件
    if cursor is not None:
        cursor_v, cursor_id = cursor
        if sort_val == "cost_difference-desc":
            final_stmt = final_stmt.where(
                (agg_stmt.c.cost_difference < cursor_v)
                | ((agg_stmt.c.cost_difference == cursor_v) & (agg_stmt.c.transition_url_id < cursor_id))
            )
        elif sort_val == "cost_difference-asc":
            final_stmt = final_stmt.where(
                (agg_stmt.c.cost_difference > cursor_v)
                | ((agg_stmt.c.cost_difference == cursor_v) & (agg_stmt.c.transition_url_id > cursor_id))
            )
        elif sort_val == "creation_time-desc" and cursor_v is not None:
            try:
                cursor_dt = cursor_v if isinstance(cursor_v, datetime) else datetime.fromisoformat(str(cursor_v).replace("Z", "+00:00"))
            except (TypeError, ValueError):
                cursor_dt = None
            if cursor_dt is not None:
                final_stmt = final_stmt.where(
                    (rep_filtered.c.creation_time < cursor_dt)
                    | ((rep_filtered.c.creation_time == cursor_dt) & (agg_stmt.c.transition_url_id < cursor_id))
                )
        elif sort_val == "creation_time-asc" and cursor_v is not None:
            try:
                cursor_dt = cursor_v if isinstance(cursor_v, datetime) else datetime.fromisoformat(str(cursor_v).replace("Z", "+00:00"))
            except (TypeError, ValueError):
                cursor_dt = None
            if cursor_dt is not None:
                final_stmt = final_stmt.where(
                    (rep_filtered.c.creation_time > cursor_dt)
                    | ((rep_filtered.c.creation_time == cursor_dt) & (agg_stmt.c.transition_url_id > cursor_id))
                )

    if sort_val == "cost_difference-desc":
        final_stmt = final_stmt.order_by(agg_stmt.c.cost_difference.desc(), agg_stmt.c.transition_url_id.desc())
    elif sort_val == "cost_difference-asc":
        final_stmt = final_stmt.order_by(agg_stmt.c.cost_difference.asc(), agg_stmt.c.transition_url_id.asc())
    elif sort_val == "creation_time-desc":
        final_stmt = final_stmt.order_by(rep_filtered.c.creation_time.desc(), agg_stmt.c.transition_url_id.desc())
    elif sort_val == "creation_time-asc":
        final_stmt = final_stmt.order_by(rep_filtered.c.creation_time.asc(), agg_stmt.c.transition_url_id.asc())
    else:
        final_stmt = final_stmt.order_by(agg_stmt.c.cost_difference.desc(), agg_stmt.c.transition_url_id.desc())

    final_stmt = final_stmt.limit(fetch_limit)
    t0 = time.perf_counter()
    result = db.execute(final_stmt)
    rows = result.mappings().all()
    query_seconds = time.perf_counter() - t0
    has_next = len(rows) > limit
    if has_next:
        rows = rows[:limit]
    next_cursor = None
    if has_next and rows:
        last = dict(rows[-1])
        tid = last.get("transition_url_id")
        if tid is not None:
            if sort_val in ("cost_difference-desc", "cost_difference-asc"):
                v = last.get("cost_difference")
                if v is not None:
                    next_cursor = _encode_cursor(float(v), int(tid))
            elif sort_val in ("creation_time-desc", "creation_time-asc"):
                v = last.get("creation_time")
                if v is not None:
                    next_cursor = _encode_cursor(v.isoformat() if hasattr(v, "isoformat") else v, int(tid))
    results = [_row_to_item(dict(r)) for r in rows]
    return TransitionUrlItemStatisticsListResponse(results=results, next=next_cursor), query_seconds
