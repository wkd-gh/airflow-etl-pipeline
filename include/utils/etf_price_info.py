from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from include.utils.common.databricks_helper import _get_databricks_conn, _get_start_dt, _merge_records
from include.utils.common.data_go_kr_helper import _fetch_all_items
from include.utils.common.secret_manager_helper import get_secret
import pandas as pd
import io

TABLE = "money_digger.equity_derivative.etf_price_info"
GCS_PREFIX = "GetSecuritiesProductInfoService"
API_URL = "https://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService/getETFPriceInfo"
UPSERT_KEYS = ["isinCd", "basDt"]


def _extract_and_upload_to_gcs(ds):
    start_dt = _get_start_dt(TABLE)

    if start_dt is None:
        raise AirflowFailException(
            f"[{TABLE}] Databricks 테이블에 초기 데이터가 없습니다. "
            "first_request/ 스크립트로 초기 CSV 적재를 먼저 수행하세요."
        )

    items = _fetch_all_items(API_URL, start_dt)

    if not items:
        raise AirflowSkipException("새로운 데이터 없음, 스킵")

    df = pd.DataFrame(items)
    json_buffer = io.StringIO()
    df.to_json(json_buffer, orient='records', force_ascii=False)

    gcs_bucket = get_secret("GCS_BUCKET")
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
    gcs_hook.upload(
        bucket_name=gcs_bucket,
        object_name=f"{GCS_PREFIX}/etf_price_info_{ds}.json",
        data=json_buffer.getvalue(),
        mime_type='application/json'
    )

    print(f"✅ GCS 업로드 완료: {len(items)}건 → gs://{gcs_bucket}/{GCS_PREFIX}/etf_price_info_{ds}.json")


def _load_gcs_to_databricks(ds):
    import json

    gcs_bucket = get_secret("GCS_BUCKET")
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
    raw_data = gcs_hook.download(
        bucket_name=gcs_bucket,
        object_name=f"{GCS_PREFIX}/etf_price_info_{ds}.json"
    )
    records = json.loads(raw_data)

    if not records:
        raise AirflowFailException(f"GCS 파일이 비어있습니다: etf_price_info_{ds}.json")

    db_conn = _get_databricks_conn()
    cursor = db_conn.cursor()

    try:
        cols_def = ", ".join([f"`{k}` STRING" for k in records[0].keys()]) + ", `_loaded_at` STRING"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE}
            ({cols_def})
            USING DELTA
        """)

        _merge_records(cursor, TABLE, records, UPSERT_KEYS, ds)
        print(f"✅ {len(records)}건 MERGE 완료 (ds={ds})")

    finally:
        cursor.close()
        db_conn.close()
