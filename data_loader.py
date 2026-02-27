"""Parquet を取得し、ローカルキャッシュに保存する。R2（data/2026-02-27/ 固定）またはローカルファイル。"""

import time
from pathlib import Path

from config import (
    CACHE_DIR,
    get_parquet_cache_path,
    get_r2_parquet_key,
    r2_configured,
)
from r2_storage import R2Storage


def ensure_data_path() -> Path:
    """
    R2 が設定されていれば data/2026-02-27/{R2_OBJECT_KEY} から取得、なければローカル Parquet を使用。
    返り値は読み込み用の Parquet ファイルパス。
    """
    cache_path = get_parquet_cache_path()
    if r2_configured():
        if not cache_path.exists():
            _download_from_r2(cache_path)
    if not cache_path.exists():
        raise FileNotFoundError(
            f"データファイルがありません: {cache_path}. "
            "R2 の環境変数（R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, BUCKET_NAME）を設定するか、"
            "data/ に transition_url_item_statistics.parquet を置いてください。"
        )
    return cache_path


def _download_from_r2(dest: Path) -> None:
    import botocore.exceptions

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = get_r2_parquet_key()
    t0 = time.perf_counter()
    storage = R2Storage()
    try:
        storage.download_file(key, str(dest))
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            raise FileNotFoundError(
                f"R2 にオブジェクトがありません: s3://{storage.bucket_name}/{key}\n"
                "  .env の R2_OBJECT_KEY（ファイル名）を確認してください。"
            ) from e
        raise
    print(f"[data_loader] R2 から取得: {time.perf_counter() - t0:.2f} 秒 -> {dest}")
