# duckdb-demo

uv と DuckDB を使ったデータ分析のデモプロジェクトです。

## セットアップ

```bash
# 依存関係は uv add 済み。初回のみ
uv sync
```

## 実行

```bash
uv run python main.py
```

## 内容

- **main.py**: サンプル売上 CSV を DuckDB で読み込み、集計・分析結果を表示
- **data/sales.csv**: サンプル売上データ（日付・商品・カテゴリ・数量・単価・地域）

## 分析例

- 全体の売上合計・数量・件数
- カテゴリ別売上
- 地域別売上
- 商品別売上トップ5

自分の CSV で試す場合は、`main.py` の `data_path` を変更するか、`read_csv_auto()` に渡すパスを編集してください。
