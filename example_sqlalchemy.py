"""
SQLAlchemy で DuckDB を操作する例（Core / 生 SQL）。
事前に main.py を1回実行して data/*.duckdb を作っておくか、
duckdb:///:memory: でインメモリ DB を指定してください。
"""

from pathlib import Path

from sqlalchemy import create_engine, text

# 永続 DB ファイル（main.py で作ったキャッシュ）
root = Path(__file__).resolve().parent
db_path = root / "data" / "transition_url_item_statistics.duckdb"

# 接続 URL（ファイルが無ければ :memory: にすると新規インメモリ DB）
if db_path.exists():
    url = f"duckdb:///{db_path}"
else:
    url = "duckdb:///:memory:"
    print("（data/*.duckdb がないためインメモリで接続。main.py を先に実行するとファイルで接続できます）")

engine = create_engine(url)

with engine.connect() as conn:
    # 生 SQL で実行
    result = conn.execute(
        text("SELECT transition_url_id, COUNT(*) AS cnt FROM transition_url_item_statistics GROUP BY 1 LIMIT 5")
    )
    for row in result:
        print(row)

# SQLAlchemy Core でテーブルを扱う場合（text の代わりに select なども可）
# from sqlalchemy import MetaData, Table, select
# metadata = MetaData()
# t = Table("transition_url_item_statistics", metadata, autoload_with=engine)
# with engine.connect() as conn:
#     for row in conn.execute(select(t.c.transition_url_id).limit(5)):
#         print(row)
