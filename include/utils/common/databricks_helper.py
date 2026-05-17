from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta

MERGE_BATCH_SIZE = 500


def _get_databricks_conn():
    """Databricks 커넥션 정보로 연결 반환"""
    from databricks import sql as databricks_sql
    conn = BaseHook.get_connection('databricks_conn')
    db_conn = databricks_sql.connect(
        server_hostname=conn.host,
        http_path=conn.extra_dejson.get('http_path'),
        access_token=conn.password
    )
    return db_conn


def _get_start_dt(table: str, buffer_days: int = 3):
    """
    Databricks 테이블에서 마지막으로 적재된 basDt 조회
    - 테이블 없거나 데이터 없으면 None 반환 (full load)
    - 있으면 buffer_days일 전 날짜 반환 (incremental)

    Args:
        table: Databricks 테이블명 (e.g. 'money_digger.equity_derivative.item_info')
        buffer_days: 마지막 적재일 기준 며칠 전부터 재수집할지 (default: 3)
    """
    catalog, schema, table_name = table.split(".")

    db_conn = _get_databricks_conn()
    cursor = db_conn.cursor()

    try:
        # 테이블 존재 여부 확인 (Unity Catalog: 카탈로그 명시 필수)
        cursor.execute(f"""
            SELECT COUNT(*) FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'
        """)
        exists = cursor.fetchone()[0]

        if not exists:
            print(f"[{table}] 테이블 없음 → Full load")
            return None

        cursor.execute(f"SELECT CAST(MAX(basDt) AS STRING) FROM {table}")
        last_dt = cursor.fetchone()[0]

        if not last_dt:
            print(f"[{table}] 데이터 없음 → Full load")
            return None

        # 버퍼 기간 적용
        start_date = datetime.strptime(str(last_dt), "%Y%m%d") - timedelta(days=buffer_days)
        start_dt = start_date.strftime("%Y%m%d")
        print(f"[{table}] Incremental load → {last_dt} 기준 {buffer_days}일 전({start_dt})부터 재수집")
        return start_dt

    finally:
        cursor.close()
        db_conn.close()


def _merge_records(cursor, table: str, records: list, upsert_keys: list, ds: str, batch_size: int = MERGE_BATCH_SIZE):
    """레코드를 batch_size 단위로 나눠 MERGE upsert (단일 거대 SQL 방지)."""
    cols = list(records[0].keys()) + ['_loaded_at']
    cols_str = ", ".join([f"`{c}`" for c in cols])
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in upsert_keys])
    source_cols = ", ".join([f"source.`{c}`" for c in cols])
    total = len(records)
    total_batches = -(-total // batch_size)

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        all_vals = []
        for record in batch:
            vals = [f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in record.values()] + [f"'{ds}'"]
            all_vals.append(f"({', '.join(vals)})")

        cursor.execute(f"""
            MERGE INTO {table} AS target
            USING (
                SELECT * FROM (VALUES {', '.join(all_vals)}) AS t({cols_str})
            ) AS source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET target.`_loaded_at` = source.`_loaded_at`
            WHEN NOT MATCHED THEN
                INSERT ({cols_str}) VALUES ({source_cols})
        """)
        print(f"  배치 {i // batch_size + 1}/{total_batches} 완료 ({min(i + batch_size, total)}/{total}건)")