# Transition URL Item Statistics API（R2 + DuckDB + SQLAlchemy）

R2 に置いた約 2GB の Parquet を取得し、DuckDB に載せて SQLAlchemy で動的クエリする軽量 API。

## ファイル構成

| ファイル | 役割 |
|----------|------|
| `config.py` | R2・キャッシュディレクトリの設定（環境変数） |
| `data_loader.py` | R2 から Parquet を取得してローカルにキャッシュ |
| `database.py` | DuckDB 接続・テーブル準備（Parquet → .duckdb） |
| `schemas.py` | 検索クエリ・レスポンスの Pydantic モデル |
| `crud.py` | SQLAlchemy で動的クエリ構築・keyset ページネーション |
| `router.py` | FastAPI ルート（GET 一覧） |
| `main_api.py` | FastAPI アプリ起動用 |

## 環境変数（.env または export）

```bash
# R2 を利用する場合
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_ENDPOINT=...
BUCKET_NAME=...
# R2_OBJECT_KEY=transition_url_item_statistics.parquet

# 省略時は data/transition_url_item_statistics.parquet を参照
```

## 起動

```bash
uv run uvicorn main_api:app --reload
```

- 初回: R2 から Parquet を取得（またはローカル `data/*.parquet` を使用）→ DuckDB に投入
- 2 回目以降: キャッシュ済み DuckDB を利用して高速応答

## エンドポイント

- `GET /transition_url_item_statistics`  
  クエリパラメータ: `limit`, `next`（カーソル）, `sort`, `app_id`, `is_affiliate`, `media_type`, `product_id`, `genre_id`, `transition_type_id`  
  レスポンスヘッダー: `X-Elapsed-Seconds`（リクエスト全体）, `X-Query-Seconds`（DuckDB クエリのみ）
- `GET /health`  
  死活確認

## 応答が遅い場合（2〜3 秒かかる理由）

- **クエリが重い**: 全件を `transition_url_id` で GROUP BY し、`row_number()` で代表行を出して JOIN しているため、テーブル全体をスキャンしています。データ量に比例して時間がかかります。
- **ディスク I/O**: DuckDB ファイル（.duckdb）をディスクから読むため、メモリだけより遅くなります。
- **対策の例**: 絞り込み条件（`app_id`, `product_id` など）を付けるとスキャン量が減りやすく、同じクエリを何度も使う場合は集約結果を別テーブルに事前計算しておく方法もあります。

## 流れ

1. R2 に `transition_url_item_statistics.parquet`（約 2GB）をアップロード
2. 起動時に `data_loader` が R2 から取得（またはローカル Parquet を使用）
3. `database` が Parquet を DuckDB の永続ファイルに投入（初回 or 更新時のみ）
4. リクエストごとに `crud` が SQLAlchemy で条件・ソート・カーソルを組み立てて DuckDB にクエリ
