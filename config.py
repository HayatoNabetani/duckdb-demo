"""R2・キャッシュ・DuckDB の設定（環境変数から読み込み）"""

import os
from pathlib import Path

from dotenv import load_dotenv

# プロジェクトルート
ROOT = Path(__file__).resolve().parent
# .env を読み込む（ROOT 直下の .env）
load_dotenv(ROOT / ".env")

# Cloudflare R2（S3 互換）.env の変数名に合わせる
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_ENDPOINT = os.getenv("R2_ENDPOINT", "")
BUCKET_NAME = os.getenv("BUCKET_NAME", "")
R2_BUCKET = BUCKET_NAME  # コード内では R2_BUCKET で参照
# R2 上の Parquet パス: data/2026-02-27/ は固定、ファイル名だけ env で指定
R2_DATA_PREFIX = "data/2026-02-27"
R2_OBJECT_KEY = os.getenv("R2_OBJECT_KEY", "transition_url_item_statistics_upload.parquet")


def get_r2_parquet_key() -> str:
    """R2 の Parquet オブジェクトキー（data/2026-02-27/ファイル名）。"""
    return f"{R2_DATA_PREFIX}/{R2_OBJECT_KEY}"


# Cloudflare API（必要に応じて）
CLOUDFLARE_EMAIL = os.getenv("CLOUDFLARE_EMAIL", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ZONE_ID = os.getenv("CLOUDFLARE_ZONE_ID", "")
# R2 バケットの公開 CDN URL（例: https://assets.example.com）
CDN_BASE_URL = os.getenv("CDN_BASE_URL", "")

# ローカルキャッシュ（R2 から落としたファイル・DuckDB）
CACHE_DIR = Path(os.getenv("CACHE_DIR", str(ROOT / "data")))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# キャッシュファイルパス
def get_parquet_cache_path() -> Path:
    return CACHE_DIR / "transition_url_item_statistics.parquet"

def get_duckdb_path() -> Path:
    return CACHE_DIR / "transition_url_item_statistics.duckdb"

def r2_configured() -> bool:
    return bool(R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_BUCKET and R2_ENDPOINT)
