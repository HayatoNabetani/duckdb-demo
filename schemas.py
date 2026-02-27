"""API のリクエスト・レスポンス用 Pydantic スキーマ（簡易版）"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ----- 検索クエリ（クエリパラメータ） -----


class TransitionUrlItemStatisticsSearchQuery(BaseModel):
    """transition_url_item_statistics 一覧の絞り込み・ソート・ページネーション"""

    limit: int = Field(100, ge=1, le=500, description="取得件数")
    next: Optional[str] = Field(None, description="keyset ページネーション用カーソル")
    sort: Optional[str] = Field(
        "cost_difference-desc",
        description="ソート: cost_difference-desc, cost_difference-asc, creation_time-desc, creation_time-asc",
    )
    app_id: Optional[str] = Field(None, description="カンマ区切り app_id")
    is_affiliate: Optional[bool] = None
    media_type: Optional[str] = Field(None, description="video / banner / carousel")
    product_id: Optional[str] = Field(None, description="カンマ区切り product_id")
    genre_id: Optional[str] = Field(None, description="カンマ区切り genre_id")
    transition_type_id: Optional[str] = Field(None, description="カンマ区切り transition_type_id")


# ----- 1件分のレスポンス -----


class TransitionUrlItemStatisticsItem(BaseModel):
    """集約＋代表行を結合した1件"""

    aggregation_time: Optional[Any] = None
    app_id: Optional[int] = None
    is_affiliate: Optional[bool] = None
    product_id: Optional[int] = None
    genre_id: Optional[int] = None
    product_name: Optional[str] = None
    genre_name: Optional[str] = None
    thumbnail_url: Optional[str] = None
    transition_url_id: Optional[int] = None
    transition_url: Optional[str] = None
    creation_time: Optional[datetime] = None
    streaming_periods: Optional[Any] = None
    transition_type_id: Optional[int] = None
    transition_type: Optional[str] = None
    account_name: Optional[str] = None
    account_url: Optional[str] = None
    account_icon_url: Optional[str] = None
    play_count: Optional[float] = None
    cost: Optional[float] = None
    max_play_count: Optional[float] = None
    max_cost: Optional[float] = None
    play_count_difference: Optional[float] = None
    cost_difference: Optional[float] = None
    max_play_count_difference: Optional[float] = None
    max_cost_difference: Optional[float] = None
    cost_difference_count: Optional[float] = None
    video_count: Optional[int] = None
    banner_count: Optional[int] = None
    carousel_count: Optional[int] = None

    class Config:
        from_attributes = True


# ----- 一覧レスポンス -----


class TransitionUrlItemStatisticsListResponse(BaseModel):
    """一覧 API のレスポンス"""

    results: list[TransitionUrlItemStatisticsItem] = Field(default_factory=list)
    next: Optional[str] = Field(None, description="次ページ用カーソル")
