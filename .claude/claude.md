# equity-derivative-etl

## 프로젝트 개요
공공데이터포털 금융위원회 API에서 데이터를 수집하여 GCS에 raw 파일로 저장하고 Databricks Delta Table에 적재하는 ETL 파이프라인

## 기술 스택
- **Orchestration**: Apache Airflow 3.2.x (Celery Executor)
- **Storage**: Google Cloud Storage (raw 파일 보관)
- **Data Warehouse**: Databricks (Delta Lake, Free Edition / AWS)
- **Source**: 공공데이터포털 금융위원회 API (data.go.kr)
- **Language**: Python 3.12

## 프로젝트 구조
```
equity-derivative-etl/
├── dags/                          # Airflow DAG 파일
├── include/
│   └── utils/
│       ├── common/
│       │   ├── slack_helper.py      # Slack 실패 알림 콜백
│       │   ├── databricks_helper.py  # Databricks 공용 함수
│       │   └── data_go_kr_helper.py  # 공공데이터포털 API 공용 함수
│       └── {테이블명}.py  # 서비스별 ETL 로직
├── first_request/                 # 초기 full load용 일회성 스크립트
│   ├── csv/                       # 추출된 CSV 파일
│   └── extract_*.py
├── docker-compose.yaml
├── dockerfile
└── requirements.txt
```

## 새 DAG 추가 시 규칙

### 1. 파일 구조
새 DAG을 추가할 때는 항상 두 파일을 함께 만든다:
- `dags/{테이블명}_dag.py` — DAG 정의 (얇은 wrapper)
- `include/utils/{테이블명}.py` — ETL 비즈니스 로직

### 2. DAG 파일 패턴
반드시 `dags/item_info_dag.py`를 참고하여 작성:
- import는 `airflow.providers.standard.operators.python.PythonOperator` 사용 (deprecated된 `airflow.operators.python` 사용 금지)
- `AirflowFailException`은 `airflow.sdk.exceptions` 에서 import
- `default_args`에 항상 `on_failure_callback: slack_failed_callback` 포함
- `start_date`는 항상 과거 날짜로 설정
- `catchup=False`, `max_active_runs=1` 필수
- DAG 파일에는 비즈니스 로직 작성 금지, 얇은 wrapper만 유지

### 3. ETL 로직 파일 패턴
반드시 `include/utils/item_info.py`를 참고하여 작성:
- 공용 함수는 직접 구현하지 말고 반드시 import해서 사용
  - `from include.utils.common.databricks_helper import _get_databricks_conn, _get_start_dt`
  - `from include.utils.common.data_go_kr_helper import _fetch_all_items`
- 함수는 `_extract_and_upload_to_gcs(ds)`와 `_load_gcs_to_databricks(ds)` 두 개로 구성
- GCS 경로: `{ServiceName}/{테이블명}_{ds}.json`
- Databricks 테이블: `money_digger.equity_derivative.{테이블명}`
- 적재 방식: 반드시 MERGE upsert 사용 (INSERT 금지)
- `UPSERT_KEYS`는 `["isinCd", "basDt"]` 기준

### 4. 공용 함수 규칙
- `_get_databricks_conn()`: Airflow 커넥션 `databricks_conn`에서 정보 가져옴
- `_get_start_dt(table)`: `MAX(basDt)` 조회 후 `BUFFER_DAYS`(3일) 전 날짜 반환, 없으면 None (full load)
- `_fetch_all_items(url, start_dt)`: 페이지네이션으로 전체 수집, `beginBasDt` 파라미터 지원

### 5. Airflow 커넥션
- GCS: `google_cloud_conn`
- Databricks: `databricks_conn`
- Slack: `slack_webhook_conn`

### 6. 환경변수
- `GCS_BUCKET`: GCS 버킷명
- `DATA_GO_KR_API_KEY`: 공공데이터포털 API 키

### 7. Databricks 테이블 스키마 규칙
- 마지막 컬럼은 항상 `_loaded_at STRING` 추가
- `USING DELTA` 필수

## 공공데이터포털 API 패턴
```
Base URL: https://apis.data.go.kr/1160100/service/{ServiceName}/{endpoint}
공통 파라미터: serviceKey, resultType=json, numOfRows=10000, pageNo, beginBasDt
응답 구조: response.body.items.item (list 또는 단건 dict)
최대 numOfRows: 10000
```

## 주의사항
- Airflow 3.x 환경이므로 2.x deprecated API 사용 금지
- Databricks Free Edition이라 External Location 사용 불가 → COPY INTO 사용 금지
- GCS → Databricks 직접 연동 불가 → Python으로 GCS 읽어서 Databricks에 적재
- full load 시 메모리 이슈 주의 (398만건 이상) → 초기 데이터는 수동 CSV 적재로 처리