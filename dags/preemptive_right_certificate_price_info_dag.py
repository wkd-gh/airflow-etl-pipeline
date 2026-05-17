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
    from include.utils.preemptive_right_certificate_price_info import (
        _extract_and_upload_to_gcs,
        _load_gcs_to_databricks,
    )
except Exception as e:
    raise AirflowFailException(f"Cannot import python_func from include/utils/preemptive_right_certificate_price_info.py: {e}")


KST = pendulum.timezone("Asia/Seoul")


def _upload_to_gcs(**context):
    _extract_and_upload_to_gcs(ds=context['ds'])


def _load_to_databricks(**context):
    _load_gcs_to_databricks(ds=context['ds'])


with DAG(
    dag_id='preemptive_right_certificate_price_info',
    description="금융위원회_신주인수권증서가격정보 API 데이터를 GCS에 업로드하고 Databricks로 적재",
    schedule="59 23 * * *",
    start_date=pendulum.datetime(2026, 5, 1, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "wkd_gh",
        "on_failure_callback": slack_failed_callback,
    },
    on_success_callback=slack_success_callback,
    tags=["GetStockSecuritiesInfoService", "databricks", "GCS", "preemptive_right_certificate_price_info"],
) as dag:

    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=_upload_to_gcs
    )

    load_to_databricks = PythonOperator(
        task_id='load_to_databricks',
        python_callable=_load_to_databricks
    )

    upload_to_gcs >> load_to_databricks
