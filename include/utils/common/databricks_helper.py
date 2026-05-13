from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta


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
        # 테이블 존재 여부 확인
        cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog = '{catalog}'
            AND table_schema = '{schema}'
            AND table_name = '{table_name}'
        """)
        exists = cursor.fetchone()[0]

        if not exists:
            print(f"[{table}] 테이블 없음 → Full load")
            return None

        cursor.execute(f"SELECT MAX(basDt) FROM {table}")
        last_dt = cursor.fetchone()[0]

        if not last_dt:
            print(f"[{table}] 데이터 없음 → Full load")
            return None

        # 버퍼 기간 적용
        start_date = datetime.strptime(last_dt, "%Y%m%d") - timedelta(days=buffer_days)
        start_dt = start_date.strftime("%Y%m%d")
        print(f"[{table}] Incremental load → {last_dt} 기준 {buffer_days}일 전({start_dt})부터 재수집")
        return start_dt

    finally:
        cursor.close()
        db_conn.close()