# equity-derivative-etl

<p align="left">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=Python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-3.2.1-017CEE?style=flat-square&logo=Apache%20Airflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=Databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Google%20Cloud%20Storage-4285F4?style=flat-square&logo=Google%20Cloud&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=Docker&logoColor=white"/>
</p>

---

## English Version

### Project Overview
**equity-derivative-etl** is an ETL pipeline that collects financial data from the Financial Services Commission (FSC) of Korea via the Public Data Portal (data.go.kr) Open API, stores raw JSON files in Google Cloud Storage (GCS), and loads them into Databricks Delta Tables for analytical use.

### Pipeline Flow

```
[FSC Open API] ──► [GCS (raw JSON)] ──► [Databricks Delta Table]
  data.go.kr          daily snapshot       money_digger.equity_derivative.*
```

1. **Extract**: Fetch data from FSC Open APIs with pagination (`numOfRows=10000`). Supports incremental loads using `MAX(basDt)` as a watermark.
2. **Upload**: Write raw JSON to GCS at `{ServiceName}/{table}_{ds}.json`.
3. **Load**: Read JSON from GCS in Python and MERGE upsert into Databricks Delta tables on keys `["isinCd", "basDt"]`.

### Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.12 |
| Orchestration | Apache Airflow 3.2.1 (Celery Executor) |
| Message Broker | Redis 7.2 |
| Metadata DB | PostgreSQL 16 |
| Raw Storage | Google Cloud Storage |
| Data Warehouse | Databricks (Delta Lake, Free Edition / AWS) |
| Data Source | Public Data Portal — FSC Open API |
| Containerization | Docker / Docker Compose |
| Alerting | Slack (webhook) |

### DAG Pipelines

| DAG ID | API Service | Table |
|---|---|---|
| `item_info` | GetKrxListedInfoService | `equity_derivative.item_info` |
| `stock_price_info` | GetStockSecuritiesInfoService | `equity_derivative.stock_price_info` |
| `securities_price_info` | GetSecuritiesInfoService | `equity_derivative.securities_price_info` |
| `etf_price_info` | GetETFSecuritiesInfoService | `equity_derivative.etf_price_info` |
| `preemptive_right_securities_price_info` | GetPreemptiveRightSecuritiesInfoService | `equity_derivative.preemptive_right_securities_price_info` |
| `preemptive_right_certificate_price_info` | GetPreemptiveRightCertificateInfoService | `equity_derivative.preemptive_right_certificate_price_info` |

All DAGs run daily at **23:59 KST** with `catchup=False` and `max_active_runs=1`.

### Project Structure

```
equity-derivative-etl/
├── dags/
│   ├── item_info_dag.py
│   ├── stock_price_info_dag.py
│   ├── securities_price_info_dag.py
│   ├── etf_price_info_dag.py
│   ├── preemptive_right_securities_price_info_dag.py
│   └── preemptive_right_certificate_price_info_dag.py
├── include/
│   └── utils/
│       ├── common/
│       │   ├── slack_helper.py          # Slack failure alert callback
│       │   ├── databricks_helper.py     # Databricks shared utilities
│       │   └── data_go_kr_helper.py     # FSC API pagination helper
│       ├── item_info.py
│       ├── stock_price_info.py
│       ├── securities_price_info.py
│       ├── etf_price_info.py
│       ├── preemptive_right_securities_price_info.py
│       └── preemptive_right_certificate_price_info.py
├── first_request/                       # One-time full load scripts
│   ├── extract_*.py
│   └── csv/                             # Extracted CSV files (initial load)
├── config/
│   └── airflow.cfg
├── docker-compose.yaml
├── dockerfile
├── requirements.txt
└── .env.example
```

### Setup

#### 1. Clone & configure environment variables

```bash
git clone <repo-url>
cd equity-derivative-etl
cp .env.example .env
```

Edit `.env`:

| Variable | Description |
|---|---|
| `AIRFLOW_UID` | Host user UID (run `id -u` to get it) |
| `AIRFLOW_UI_USERNAME` | Airflow web UI admin username |
| `AIRFLOW_UI_PASSWORD` | Airflow web UI admin password |
| `DATA_GO_KR_API_KEY` | FSC Open API key from data.go.kr |
| `FERNET_KEY` | Airflow Fernet encryption key |
| `GCS_BUCKET` | GCS bucket name for raw JSON storage |
| `PYTHONPATH` | Set to `/opt/airflow` inside Docker |

#### 2. Build and start

```bash
docker compose up --build -d
```

#### 3. Configure Airflow connections

After the services are up, register the following connections via the Airflow UI (`Admin > Connections`):

| Conn ID | Type | Purpose |
|---|---|---|
| `google_cloud_conn` | Google Cloud | GCS access |
| `databricks_conn` | Databricks | Delta table access |
| `slack_webhook_conn` | HTTP | Slack failure alerts |

#### 4. Access the Airflow UI

Navigate to `http://localhost:8080` and log in with the credentials set in `.env`.

---

## 한국어 버전

### 프로젝트 개요
**equity-derivative-etl**은 공공데이터포털(data.go.kr) 금융위원회 API에서 금융 데이터를 수집하여 Google Cloud Storage(GCS)에 raw JSON으로 저장하고, Databricks Delta Table에 적재하는 ETL 파이프라인입니다.

### 파이프라인 흐름

```
[금융위원회 API] ──► [GCS (raw JSON)] ──► [Databricks Delta Table]
  data.go.kr           일별 스냅샷         money_digger.equity_derivative.*
```

1. **Extract (추출)**: 금융위원회 Open API를 페이지네이션(`numOfRows=10000`)으로 전체 수집. `MAX(basDt)` 기준으로 증분 적재 지원.
2. **Upload (업로드)**: raw JSON을 GCS `{ServiceName}/{테이블명}_{ds}.json` 경로에 저장.
3. **Load (적재)**: GCS에서 JSON을 Python으로 읽어 `["isinCd", "basDt"]` 키 기준 MERGE upsert로 Databricks Delta Table에 적재.

### 기술 스택

| 구성 요소 | 기술 |
|---|---|
| 언어 | Python 3.12 |
| 워크플로우 관리 | Apache Airflow 3.2.1 (Celery Executor) |
| 메시지 브로커 | Redis 7.2 |
| 메타데이터 DB | PostgreSQL 16 |
| Raw 스토리지 | Google Cloud Storage |
| 데이터 웨어하우스 | Databricks (Delta Lake, Free Edition / AWS) |
| 데이터 소스 | 공공데이터포털 금융위원회 Open API |
| 컨테이너화 | Docker / Docker Compose |
| 알림 | Slack (webhook) |

### DAG 파이프라인

| DAG ID | API 서비스 | 테이블 |
|---|---|---|
| `item_info` | GetKrxListedInfoService | `equity_derivative.item_info` |
| `stock_price_info` | GetStockSecuritiesInfoService | `equity_derivative.stock_price_info` |
| `securities_price_info` | GetSecuritiesInfoService | `equity_derivative.securities_price_info` |
| `etf_price_info` | GetETFSecuritiesInfoService | `equity_derivative.etf_price_info` |
| `preemptive_right_securities_price_info` | GetPreemptiveRightSecuritiesInfoService | `equity_derivative.preemptive_right_securities_price_info` |
| `preemptive_right_certificate_price_info` | GetPreemptiveRightCertificateInfoService | `equity_derivative.preemptive_right_certificate_price_info` |

모든 DAG은 **매일 23:59 (KST)** 스케줄로 실행되며 `catchup=False`, `max_active_runs=1`로 설정되어 있습니다.

### 프로젝트 구조

```
equity-derivative-etl/
├── dags/
│   ├── item_info_dag.py
│   ├── stock_price_info_dag.py
│   ├── securities_price_info_dag.py
│   ├── etf_price_info_dag.py
│   ├── preemptive_right_securities_price_info_dag.py
│   └── preemptive_right_certificate_price_info_dag.py
├── include/
│   └── utils/
│       ├── common/
│       │   ├── slack_helper.py          # Slack 실패 알림 콜백
│       │   ├── databricks_helper.py     # Databricks 공용 함수
│       │   └── data_go_kr_helper.py     # 공공데이터포털 API 페이지네이션 헬퍼
│       ├── item_info.py
│       ├── stock_price_info.py
│       ├── securities_price_info.py
│       ├── etf_price_info.py
│       ├── preemptive_right_securities_price_info.py
│       └── preemptive_right_certificate_price_info.py
├── first_request/                       # 초기 full load 일회성 스크립트
│   ├── extract_*.py
│   └── csv/                             # 추출된 CSV 파일 (초기 수동 적재용)
├── config/
│   └── airflow.cfg
├── docker-compose.yaml
├── dockerfile
├── requirements.txt
└── .env.example
```

### 환경 설정

#### 1. 저장소 클론 및 환경변수 설정

```bash
git clone <repo-url>
cd equity-derivative-etl
cp .env.example .env
```

`.env` 파일 편집:

| 변수명 | 설명 |
|---|---|
| `AIRFLOW_UID` | 호스트 사용자 UID (`id -u` 명령으로 확인) |
| `AIRFLOW_UI_USERNAME` | Airflow 웹 UI 관리자 계정명 |
| `AIRFLOW_UI_PASSWORD` | Airflow 웹 UI 관리자 비밀번호 |
| `DATA_GO_KR_API_KEY` | 공공데이터포털 API 키 |
| `FERNET_KEY` | Airflow Fernet 암호화 키 |
| `GCS_BUCKET` | raw JSON 저장용 GCS 버킷명 |
| `PYTHONPATH` | Docker 내에서 `/opt/airflow`로 설정 |

#### 2. 빌드 및 실행

```bash
docker compose up --build -d
```

#### 3. Airflow 커넥션 등록

서비스 기동 후 Airflow UI(`Admin > Connections`)에서 아래 커넥션을 등록합니다:

| Conn ID | 타입 | 용도 |
|---|---|---|
| `google_cloud_conn` | Google Cloud | GCS 접근 |
| `databricks_conn` | Databricks | Delta Table 접근 |
| `slack_webhook_conn` | HTTP | Slack 실패 알림 |

#### 4. Airflow UI 접속

`http://localhost:8080` 에서 `.env`에 설정한 계정으로 로그인합니다.
