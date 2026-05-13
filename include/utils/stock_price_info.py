from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk.exceptions import AirflowFailException
from include.utils.common.databricks_helper import _get_databricks_conn, _get_start_dt
from include.utils.common.data_go_kr_helper import _fetch_all_items
import pandas as pd
import io, os


GCS_BUCKET = os.getenv("GCS_BUCKET")

TABLE = "money_digger.equity_derivative.stock_price_info"
GCS_PREFIX = "GetStockSecuritiesInfoService"
API_URL = "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"
UPSERT_KEYS = ["isinCd", "basDt"]


def _extract_and_upload_to_gcs(ds):
    start_dt = _get_start_dt(TABLE)

    items = _fetch_all_items(API_URL, start_dt)

    if not items:
        print("✅ 새로운 데이터 없음, 스킵")
        return

    df = pd.DataFrame(items)
    json_buffer = io.StringIO()
    df.to_json(json_buffer, orient='records', force_ascii=False)

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=f"{GCS_PREFIX}/stock_price_info_{ds}.json",
        data=json_buffer.getvalue(),
        mime_type='application/json'
    )

    print(f"✅ GCS 업로드 완료: {len(items)}건 → gs://{GCS_BUCKET}/{GCS_PREFIX}/stock_price_info_{ds}.json")


def _load_gcs_to_databricks(ds):
    import json

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
    raw_data = gcs_hook.download(
        bucket_name=GCS_BUCKET,
        object_name=f"{GCS_PREFIX}/stock_price_info_{ds}.json"
    )
    records = json.loads(raw_data)

    if not records:
        raise AirflowFailException(f"GCS 파일이 비어있습니다: stock_price_info_{ds}.json")

    db_conn = _get_databricks_conn()
    cursor = db_conn.cursor()

    try:
        cols_def = ", ".join([f"`{k}` STRING" for k in records[0].keys()]) + ", `_loaded_at` STRING"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE}
            ({cols_def})
            USING DELTA
        """)

        cols = list(records[0].keys()) + ['_loaded_at']
        cols_str = ", ".join([f"`{c}`" for c in cols])

        all_vals = []
        for record in records:
            vals = [f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in record.values()] + [f"'{ds}'"]
            all_vals.append(f"({', '.join(vals)})")

        merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in UPSERT_KEYS])

        cursor.execute(f"""
            MERGE INTO {TABLE} AS target
            USING (
                VALUES {', '.join(all_vals)}
            ) AS source({cols_str})
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET target.`_loaded_at` = source.`_loaded_at`
            WHEN NOT MATCHED THEN
                INSERT ({cols_str}) VALUES ({', '.join([f'source.`{c}`' for c in cols])})
        """)

        print(f"✅ {len(records)}건 MERGE 완료 (ds={ds})")

    finally:
        cursor.close()
        db_conn.close()
