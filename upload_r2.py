"""
transition_url_item_statistics.csv（または .parquet）を Cloudflare R2 にアップロードする。

アップロード先: BUCKET_NAME/data/今日の日付/ファイル名
例: my-bucket/data/2025-02-27/transition_url_item_statistics.parquet

API は Parquet を想定しているため、CSV の場合は一度 Parquet に変換してからアップロードする。

使い方:
  uv run python upload_r2.py
  uv run python upload_r2.py path/to/file.csv
  uv run python upload_r2.py path/to/file.parquet
"""

import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

from config import CACHE_DIR, r2_configured
from r2_storage import R2Storage


def make_r2_key(filename: str) -> str:
    """アップロード先キー: data/今日の日付/ファイル名"""
    today = date.today().isoformat()
    return f"data/{today}/{filename}"


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


def upload_to_r2(local_path: Path, object_key: Optional[str] = None) -> None:
    """ローカルファイルを R2 にアップロードする。キー省略時は data/今日の日付/ファイル名。"""
    if not r2_configured():
        print("エラー: R2 の環境変数が設定されていません。")
        print("  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, BUCKET_NAME を設定してください。")
        sys.exit(1)

    key = object_key or make_r2_key(local_path.name)
    storage = R2Storage()
    size_mb = local_path.stat().st_size / (1024 * 1024)
    print(f"アップロード: {local_path} -> R2 s3://{storage.bucket_name}/{key} ({size_mb:.1f} MB)")

    t0 = time.perf_counter()
    storage.upload_file(key, str(local_path))
    elapsed = time.perf_counter() - t0
    print(f"完了: {elapsed:.2f} 秒")


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
        r2_key = make_r2_key("transition_url_item_statistics.parquet")
        upload_to_r2(parquet_path, object_key=r2_key)
        parquet_path.unlink(missing_ok=True)
    else:
        upload_to_r2(source)


if __name__ == "__main__":
    main()
