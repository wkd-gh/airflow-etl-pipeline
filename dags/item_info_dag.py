from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowFailException
import os, pendulum

try:
    from include.utils.common.slack_helper import (
        slack_failed_callback,
        slack_success_callback,
    )
except Exception as e:
    raise AirflowFailException(f"Cannot import python_func from include/utils/common/slack_helper.py: {e}")

try:
    from include.utils.item_info import (
        _extract_and_upload_to_gcs,
        _load_gcs_to_databricks,
    )
except Exception as e:
    raise AirflowFailException(f"Cannot import python_func from include/utils/item_info.py: {e}")


# 설정 정보
KST = pendulum.timezone("Asia/Seoul")


def _upload_to_gcs(**context):
    """KRX 상장종목정보 API 데이터를 GCS에 업로드"""
    _extract_and_upload_to_gcs(ds=context['ds'])


def _load_to_databricks(**context):
    """GCS raw JSON → Databricks Delta Table 적재"""
    _load_gcs_to_databricks(ds=context['ds'])


with DAG(
    dag_id='item_info',
    description="금융위원회_KRX상장종목정보 API 데이터를 GCS에 업로드하고 Databricks로 적재",
    schedule="59 23 * * *",                                # 매일 23:59 (KST)
    start_date=pendulum.datetime(2026, 5, 1, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "wkd_gh",
        "on_failure_callback": slack_failed_callback,
    },
    on_success_callback=slack_success_callback,
    tags=["GetKrxListedInfoService", "databricks", "GCS", "item_info"],
) as dag:

    # Task 1: API 데이터 추출 및 GCS 적재
    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=_upload_to_gcs
    )

    # Task 2: GCS raw JSON → Databricks Delta Table 적재
    load_to_databricks = PythonOperator(
        task_id='load_to_databricks',
        python_callable=_load_to_databricks
    )

    upload_to_gcs >> load_to_databricks