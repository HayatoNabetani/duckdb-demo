"""DuckDB を使ったデータ分析デモ"""

import time
import duckdb
from pathlib import Path


def main() -> None:
    # プロジェクトルート
    root = Path(__file__).resolve().parent
    data_path = root / "data" / "transition_url_item_statistics.csv"

    if not data_path.exists():
        print(f"サンプルデータが見つかりません: {data_path}")
        return

    data_dir = data_path.parent
    parquet_path = data_path.with_suffix(".parquet")
    db_path = data_dir / "transition_url_item_statistics.duckdb"

    # ソース: Parquet があって CSV より新しければ Parquet、それ以外は CSV
    use_parquet = parquet_path.exists() and (
        not data_path.exists() or parquet_path.stat().st_mtime >= data_path.stat().st_mtime
    )
    source_path = parquet_path if use_parquet else data_path
    need_load = not db_path.exists() or source_path.stat().st_mtime > db_path.stat().st_mtime

    t_load = time.perf_counter()
    con = duckdb.connect(str(db_path))
    if need_load:
        con.execute("DROP TABLE IF EXISTS transition_url_item_statistics")
        if use_parquet:
            pq_sql = str(parquet_path).replace("'", "''")
            con.execute(
                f"CREATE TABLE transition_url_item_statistics AS SELECT * FROM read_parquet('{pq_sql}')"
            )
            print(f"⏱ データ読込 (Parquet): {time.perf_counter() - t_load:.3f} 秒")
        else:
            csv_path_sql = str(data_path).replace("'", "''")
            con.execute(
                f"CREATE TABLE transition_url_item_statistics AS SELECT * FROM read_csv_auto('{csv_path_sql}', header=true)"
            )
            print(f"⏱ データ読込 (CSV): {time.perf_counter() - t_load:.3f} 秒")
            pq_sql = str(parquet_path).replace("'", "''")
            con.execute(f"COPY transition_url_item_statistics TO '{pq_sql}' (FORMAT PARQUET)")
            print("   （次回用に Parquet キャッシュを保存しました）")
    else:
        print(f"⏱ データ読込 (キャッシュ): {time.perf_counter() - t_load:.3f} 秒")

    print("=" * 60)
    print("📊 分析レポート")
    print("=" * 60)

    t0 = time.perf_counter()
    results = con.execute(
        """
        SELECT agg_subq.aggregation_time, rep_filtered.app_id, rep_filtered.is_affiliate, rep_filtered.product_id, rep_filtered.genre_id, rep_filtered.product_name, rep_filtered.genre_name, rep_filtered.thumbnail_url, agg_subq.transition_url_id, agg_subq.transition_url, rep_filtered.creation_time, rep_filtered.streaming_periods, agg_subq.transition_type_id, agg_subq.transition_type, rep_filtered.account_name, rep_filtered.account_url, rep_filtered.account_icon_url, agg_subq.play_count, agg_subq.cost, agg_subq.max_play_count, agg_subq.max_cost, agg_subq.play_count_difference, agg_subq.cost_difference, agg_subq.max_play_count_difference, agg_subq.max_cost_difference, agg_subq.cost_difference_count, agg_subq.video_count, agg_subq.banner_count, agg_subq.carousel_count 

FROM (SELECT transition_url_item_statistics.transition_url_id AS transition_url_id, transition_url_item_statistics.transition_url AS transition_url, transition_url_item_statistics.transition_type_id AS transition_type_id, transition_url_item_statistics.transition_type AS transition_type, transition_url_item_statistics.aggregation_time AS aggregation_time, sum(transition_url_item_statistics.play_count) AS play_count, sum(transition_url_item_statistics.cost) AS cost, max(transition_url_item_statistics.max_play_count) AS max_play_count, max(transition_url_item_statistics.max_cost) AS max_cost, sum(transition_url_item_statistics.play_count_difference_2) AS play_count_difference, sum(transition_url_item_statistics.cost_difference_2) AS cost_difference, max(transition_url_item_statistics.max_play_count_difference_2) AS max_play_count_difference, max(transition_url_item_statistics.max_cost_difference_2) AS max_cost_difference, sum(transition_url_item_statistics.cost_difference_count_2) AS cost_difference_count, sum(CASE WHEN (transition_url_item_statistics.media_type = 'video') THEN 1 ELSE 0 END) AS video_count, sum(CASE WHEN (transition_url_item_statistics.media_type = 'banner') THEN 1 ELSE 0 END) AS banner_count, sum(CASE WHEN (transition_url_item_statistics.media_type = 'carousel') THEN 1 ELSE 0 END) AS carousel_count 

FROM transition_url_item_statistics GROUP BY transition_url_item_statistics.transition_url_id, transition_url_item_statistics.transition_url, transition_url_item_statistics.transition_type_id, transition_url_item_statistics.transition_type, transition_url_item_statistics.aggregation_time) AS agg_subq JOIN (SELECT rep_subq.transition_url_id AS transition_url_id, rep_subq.thumbnail_url AS thumbnail_url, rep_subq.app_id AS app_id, rep_subq.is_affiliate AS is_affiliate, rep_subq.media_type AS media_type, rep_subq.product_id AS product_id, rep_subq.genre_id AS genre_id, rep_subq.product_name AS product_name, rep_subq.genre_name AS genre_name, rep_subq.account_name AS account_name, rep_subq.account_url AS account_url, rep_subq.account_icon_url AS account_icon_url, rep_subq.creation_time AS creation_time, rep_subq.streaming_periods AS streaming_periods 

FROM (SELECT transition_url_item_statistics.transition_url_id AS transition_url_id, transition_url_item_statistics.thumbnail_url AS thumbnail_url, transition_url_item_statistics.app_id AS app_id, transition_url_item_statistics.is_affiliate AS is_affiliate, transition_url_item_statistics.media_type AS media_type, transition_url_item_statistics.product_id AS product_id, transition_url_item_statistics.genre_id AS genre_id, transition_url_item_statistics.product_name AS product_name, transition_url_item_statistics.genre_name AS genre_name, transition_url_item_statistics.account_name AS account_name, transition_url_item_statistics.account_url AS account_url, transition_url_item_statistics.account_icon_url AS account_icon_url, transition_url_item_statistics.creation_time AS creation_time, transition_url_item_statistics.streaming_periods AS streaming_periods, row_number() OVER (PARTITION BY transition_url_item_statistics.transition_url_id ORDER BY transition_url_item_statistics.cost_difference_2 DESC) AS rn 

FROM transition_url_item_statistics) AS rep_subq 

WHERE rep_subq.rn = 1) AS rep_filtered ON agg_subq.transition_url_id = rep_filtered.transition_url_id ORDER BY agg_subq.cost_difference DESC, agg_subq.transition_url_id DESC

 LIMIT 101
        """
    ).fetchall()
    elapsed = time.perf_counter() - t0
    print(f"⏱ クエリ実行時間: {elapsed:.3f} 秒\n")

    for result in results:
        print(result)

    con.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
