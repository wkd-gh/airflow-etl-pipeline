from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.exceptions import AirflowFailException
import requests
import pandas as pd
import io, os


# 설정 정보
GCS_BUCKET = os.getenv("GCS_BUCKET")      # GCS 버킷 이름
API_KEY = os.getenv("DATA_GO_KR_API_KEY") # 공공데이터 포털 API 키


def _extract_and_upload_to_gcs(ds):
    """
    금융위원회_KRX상장종목정보 를 호출하여 GCS에 업로드
    [ Base URL: apis.data.go.kr/1160100/service/GetKrxListedInfoService ]
    사이트: https://www.data.go.kr/data/15094775/openapi.do
    """
    url = "http://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo"
    params = {
        'serviceKey': API_KEY,
        'resultType': 'json',
        'numOfRows': '100',
        'pageNo': '1'
    }

    # 1. API 데이터 추출
    response = requests.get(url, params=params)
    items = response.json()['response']['body']['items']['item']
    df = pd.DataFrame(items)

    # 2. 메모리 상에서 JSON 변환
    json_buffer = io.StringIO()
    df.to_json(json_buffer, orient='records', force_ascii=False)

    # 3. GCS Hook을 사용하여 파일 업로드
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=f"GetKrxListedInfoService/item_info_{ds}.json",
        data=json_buffer.getvalue(),
        mime_type='application/json'
    )


def _load_gcs_to_databricks(ds):
    """GCS raw JSON을 읽어서 Databricks Delta Table에 적재"""
    import json
    from databricks import sql as databricks_sql

    # 1. Databricks 커넥션 정보 재활용
    conn = BaseHook.get_connection('databricks_conn')
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get('http_path')

    # 2. GCS에서 raw JSON 읽기
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn')
    raw_data = gcs_hook.download(
        bucket_name=GCS_BUCKET,
        object_name=f"GetKrxListedInfoService/item_info_{ds}.json"
    )
    records = json.loads(raw_data)

    if not records:
        raise AirflowFailException(f"GCS 파일이 비어있습니다: item_info_{ds}.json")

    # 3. Databricks 연결
    db_conn = databricks_sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token
    )
    cursor = db_conn.cursor()

    try:
        # 4. 테이블 없으면 생성 (_loaded_at 포함)
        cols_def = ", ".join([f"`{k}` STRING" for k in records[0].keys()]) + ", `_loaded_at` STRING"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS money_digger.equity_derivative.item_info
            ({cols_def})
            USING DELTA
        """)

        # 5. 날짜 기준 기존 데이터 삭제 후 재적재 (idempotent)
        cursor.execute(f"""
            DELETE FROM money_digger.equity_derivative.item_info
            WHERE _loaded_at = '{ds}'
        """)

        # 6. bulk INSERT (한 번에 전체 적재)
        cols = list(records[0].keys()) + ['_loaded_at']
        cols_str = ", ".join([f"`{c}`" for c in cols])

        all_vals = []
        for record in records:
            vals = [f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in record.values()] + [f"'{ds}'"]
            all_vals.append(f"({', '.join(vals)})")

        cursor.execute(f"""
            INSERT INTO money_digger.equity_derivative.item_info ({cols_str})
            VALUES {', '.join(all_vals)}
        """)

        print(f"✅ {len(records)}건 bulk 적재 완료 (ds={ds})")

    finally:
        cursor.close()
        db_conn.close()