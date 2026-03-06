"""
transition_url_item_statistics.csv（または .parquet）を Cloudflare R2 にアップロードする。

アップロード先:
  1. BUCKET_NAME/data/今日の日付/ファイル名  （例: my-bucket/data/2025-02-27/transition_url_item_statistics.parquet）
  2. BUCKET_NAME/data/today/ファイル名       （常に最新を指す固定パス）

data/today/ へのアップロード後、Cloudflare CDN キャッシュをパージする。

使い方:
  uv run python upload_r2.py
  uv run python upload_r2.py path/to/file.csv
  uv run python upload_r2.py path/to/file.parquet
"""

import sys
import time
from datetime import date
from pathlib import Path

import requests

from config import (
    CACHE_DIR,
    CDN_BASE_URL,
    CLOUDFLARE_API_KEY,
    CLOUDFLARE_EMAIL,
    CLOUDFLARE_ZONE_ID,
    r2_configured,
)
from r2_storage import R2Storage


def make_r2_key_dated(filename: str) -> str:
    """アップロード先キー: data/今日の日付/ファイル名"""
    today = date.today().isoformat()
    return f"data/{today}/{filename}"


def make_r2_key_today(filename: str) -> str:
    """アップロード先キー: data/today/ファイル名（常に最新を指す固定パス）"""
    return f"data/today/{filename}"


def csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    """CSV を DuckDB で読み、Parquet に書き出す。"""
    import duckdb

    path_sql = str(csv_path.resolve()).replace("'", "''")
    out_sql = str(parquet_path.resolve()).replace("'", "''")
    con = duckdb.connect(":memory:")
    con.execute(
        f"COPY (SELECT * FROM read_csv_auto('{path_sql}', header=true)) TO '{out_sql}' (FORMAT PARQUET)"
    )
    con.close()


def upload_to_r2(local_path: Path, object_key: str) -> None:
    """ローカルファイルを指定キーで R2 にアップロードする。"""
    storage = R2Storage()
    size_mb = local_path.stat().st_size / (1024 * 1024)
    print(f"アップロード: {local_path} -> R2 s3://{storage.bucket_name}/{object_key} ({size_mb:.1f} MB)")

    t0 = time.perf_counter()
    storage.upload_file(object_key, str(local_path))
    elapsed = time.perf_counter() - t0
    print(f"完了: {elapsed:.2f} 秒")


def purge_files_cache(files: list[str]) -> dict:
    """Cloudflare CDN のキャッシュを指定した URL リストでパージする。"""
    headers = {
        "Content-Type": "application/json",
        "X-Auth-Email": CLOUDFLARE_EMAIL,
        "X-Auth-Key": CLOUDFLARE_API_KEY,
    }
    response = requests.post(
        f"https://api.cloudflare.com/client/v4/zones/{CLOUDFLARE_ZONE_ID}/purge_cache",
        headers=headers,
        json={"files": files},
        timeout=30,
    )
    return response.json()


def purge_today_cache(filename: str) -> None:
    """data/today/{filename} の CDN キャッシュをパージする。"""
    if not all([CLOUDFLARE_ZONE_ID, CLOUDFLARE_API_KEY, CDN_BASE_URL]):
        print("スキップ: CLOUDFLARE_ZONE_ID / CLOUDFLARE_API_KEY / CDN_BASE_URL が未設定のためキャッシュパージをスキップします。")
        return

    today_key = make_r2_key_today(filename)
    url = f"{CDN_BASE_URL.rstrip('/')}/{today_key}"
    print(f"キャッシュパージ: {url}")

    t0 = time.perf_counter()
    result = purge_files_cache([url])
    elapsed = time.perf_counter() - t0

    if result.get("success"):
        print(f"キャッシュパージ完了: {elapsed:.2f} 秒")
    else:
        print(f"キャッシュパージ失敗: {result.get('errors', [])}")


def upload_file_to_both_paths(local_path: Path, filename: str) -> None:
    """
    ファイルを日付パスと data/today/ の両方にアップロードし、
    data/today/ のキャッシュをパージする。
    """
    if not r2_configured():
        print("エラー: R2 の環境変数が設定されていません。")
        print("  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, BUCKET_NAME を設定してください。")
        sys.exit(1)

    dated_key = make_r2_key_dated(filename)
    today_key = make_r2_key_today(filename)

    # 1. 日付フォルダへアップロード
    print("\n[1/2] 日付フォルダへアップロード")
    upload_to_r2(local_path, dated_key)

    # 2. data/today/ へアップロード
    print("\n[2/2] data/today/ へアップロード")
    upload_to_r2(local_path, today_key)

    # 3. data/today/ のキャッシュをパージ
    print("\n[CDN] data/today/ のキャッシュをパージ")
    purge_today_cache(filename)


def main() -> None:
    if len(sys.argv) >= 2:
        source = Path(sys.argv[1])
    else:
        source = CACHE_DIR / "transition_url_item_statistics.csv"

    if not source.exists():
        print(f"エラー: ファイルが見つかりません: {source}")
        print("  使い方: uv run python upload_r2.py [path/to/file.csv または file.parquet]")
        sys.exit(1)

    if source.suffix.lower() == ".csv":
        # CSV → 一時 Parquet に変換してからアップロード（R2 上は transition_url_item_statistics.parquet で統一）
        parquet_path = CACHE_DIR / "transition_url_item_statistics_upload.parquet"
        print(f"CSV を Parquet に変換中: {source}")
        t0 = time.perf_counter()
        csv_to_parquet(source, parquet_path)
        print(f"変換完了: {time.perf_counter() - t0:.2f} 秒")
        try:
            upload_file_to_both_paths(parquet_path, "transition_url_item_statistics.parquet")
        finally:
            parquet_path.unlink(missing_ok=True)
    else:
        upload_file_to_both_paths(source, source.name)


if __name__ == "__main__":
    main()
