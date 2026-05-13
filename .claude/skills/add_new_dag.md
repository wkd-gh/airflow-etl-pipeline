# 새 DAG 추가 스킬

새로운 공공데이터포털 API ETL DAG을 추가할 때 사용하는 스킬

## 입력 정보 수집
사용자에게 다음 정보를 요청:
1. API URL
2. Databricks 테이블명 (예: `etf_price_info`)
3. API 응답 스키마 (item 필드 목록)
4. UPSERT 기준 키 (기본값: `["isinCd", "basDt"]`)
5. GCS_PREFIX (예: `GetKrxListedInfoService`)

## 생성할 파일 목록
1. `dags/{테이블명}_dag.py`
2. `include/utils/{테이블명}.py`

## 체크리스트
새 DAG 추가 후 반드시 확인:
- [ ] `dags/` 파일에 비즈니스 로직 없이 wrapper만 있는가?
- [ ] 공용 함수 (`_get_databricks_conn`, `_get_start_dt`, `_fetch_all_items`) import 했는가?
- [ ] `on_failure_callback: slack_failed_callback` 포함했는가?
- [ ] `start_date`가 과거 날짜인가?
- [ ] `catchup=False`, `max_active_runs=1` 설정했는가?
- [ ] MERGE upsert 사용했는가?
- [ ] `_loaded_at STRING` 컬럼 포함했는가?

## 참고 파일
- DAG 패턴: `dags/item_info_dag.py`
- ETL 패턴: `include/utils/item_info.py`
- 공용 함수: `include/utils/common/slack_helper.py`, `include/utils/common/databricks_helper.py`, `include/utils/common/data_go_kr_helper.py`