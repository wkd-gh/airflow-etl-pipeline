from ast import Dict
import pendulum
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import logging
from datetime import timedelta

KST = pendulum.timezone("Asia/Seoul")

# Airflow owner → Slack Member ID 매핑
OWNER_SLACK_ID = {
    "wkd_gh": "U0B2W1430SV",
    "thanku": "U0B2U4S79NW"
}

# --------------------------------
# Helper Functions
# --------------------------------
def _fmt_td(sec: float) -> str:
    sec = max(0, int(sec))
    return str(timedelta(seconds=sec))

def _truncate(s: str, limit: int = 1700) -> str:
    if s is None:
        return ""
    s = str(s)
    return (s[:limit] + " …(truncated)") if len(s) > limit else s

def _owner_mention(context):
    ti = context["ti"]
    owner = getattr(ti.task, "owner", "")
    slack_id = OWNER_SLACK_ID.get(owner)
    return f"<@{slack_id}>" if slack_id else f"@{owner}"

def slack_failed_callback(context):
    """
    Airflow → Slack 실패 알림
    - Attachment를 사용하여 빨간색 라인 적용
    - 버튼에 이모지 아이콘 적용
    """
    ti = context["ti"]
    dag = context["dag"]
    dag_run = context.get("dag_run")

    # Core meta
    dag_id = dag.dag_id
    task_id = ti.task_id
    try_number = ti.try_number
    max_tries = getattr(ti, "max_tries", None) or getattr(ti.task, "retries", 0)

    # Times
    logical_date_utc = context["logical_date"]
    run_time_kst = logical_date_utc.in_timezone(KST).strftime("%Y-%m-%d %H:%M:%S")
    run_time_utc_iso = logical_date_utc.isoformat()
    
    # Data Interval
    di_start = context.get("data_interval_start")
    di_end   = context.get("data_interval_end")
    di_kst = ""
    if di_start and di_end:
        di_kst = f"{di_start.in_timezone(KST).strftime('%Y-%m-%d %H:%M:%S')} ~ {di_end.in_timezone(KST).strftime('%Y-%m-%d %H:%M:%S')}"
    
    # Runtime info
    start = ti.start_date
    end = ti.end_date or pendulum.now("UTC")
    duration = _fmt_td((end - start).total_seconds() if start else 0)

    # Owner & Tags
    owner = getattr(ti.task, "owner", "")
    tags  = ", ".join(getattr(dag, "tags", []) or [])

    # Links & error
    VM_EXTERNAL_IP = "34.50.49.51:8080"  # 실제 VM 고정 IP로 변경
    log_url = ti.log_url
    
    if "localhost" in log_url:
        log_url = log_url.replace("localhost:8080", VM_EXTERNAL_IP)
    
    err = _truncate(context.get("exception"))

    # -------------------------------------------------------
    # 1. Blocks 구성 (내용물)
    # -------------------------------------------------------
    # 헤더와 필드 정보
    main_blocks_1 = [
        {"type": "header", "text": {"type": "plain_text", "text": f":alert: DAG Failed - {dag_id}", "emoji": True}},
        {"type": "divider"},
    ]
    
    main_blocks_2 = []

    # Row 1: DAG, Task
    main_blocks_2.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*DAG*\n`{dag_id}`"},
            {"type": "mrkdwn", "text": f"*Task*\n`{task_id}`"},
        ]
    })
    
    # Row 2: Run(KST), Run(UTC)
    main_blocks_2.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*Run (KST)*\n`{run_time_kst}`"},
            {"type": "mrkdwn", "text": f"*Run (UTC)*\n`{run_time_utc_iso}`"},
        ]
    })

    # Row 3: Try, Duration
    main_blocks_2.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*Try*\n`{try_number}/{max_tries + 1}`"},
            {"type": "mrkdwn", "text": f"*Duration*\n`{duration}`"},
        ]
    })

    # Owner, Tags 정보 추가
    extra_fields = []
    if owner: extra_fields.append({"type": "mrkdwn", "text": f"*Owner*\n{_owner_mention(context)}"})
    if tags:  extra_fields.append({"type": "mrkdwn", "text": f"*Tags*\n`{tags}`"})
    
    if extra_fields:
        main_blocks_2.append({"type": "section", "fields": extra_fields})

    # 에러 로그 추가
    err_blocks = []

    if err:
        err_blocks = [{
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Error Log:*\n```{err}```"}
        }]

    # -------------------------------------------------------
    # 2. 버튼 구성 (아이콘 포함)
    # -------------------------------------------------------
    button_block = [
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View Logs  :airflow:", 
                        "emoji": True
                    },
                    "url": log_url,
                }
            ]
        }
    ]
    
    # -------------------------------------------------------
    # 3. Footer 구성 (하단 정보)
    # -------------------------------------------------------
    footer_block = []
    if di_kst:
        footer_block = [
            {"type": "divider"},
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"Money Digger by Airflow"}]
            }
        ]

    # -------------------------------------------------------
    # 3. Payload 조립 (분리형 구조)
    # -------------------------------------------------------
    attachment_main = {
        "color": "#E01E5A",
        "blocks": main_blocks_2
    }
    attachment_bottom = {
        "blocks": err_blocks + button_block
    }

    payload = {
        "blocks": main_blocks_1, 
        "attachments": [attachment_main, attachment_bottom], 
        "unfurl_links": False
    }

    try:
        SlackWebhookHook(slack_webhook_conn_id="slack_webhook_conn").send_dict(payload)
    except Exception as e:
        logging.exception("Slack callback failed: %s", e)


def slack_success_callback(context):
    """
    Airflow → Slack 성공 알림
    - Attachment를 사용하여 초록색 라인 적용
    - DAG 레벨 / Task 레벨 콜백 모두 지원 (ti 없을 수 있음)
    """
    ti = context.get("ti")  # DAG 레벨 콜백에는 ti가 없음
    dag = context["dag"]
    dag_run = context.get("dag_run")

    dag_id = dag.dag_id

    logical_date_utc = context["logical_date"]
    run_time_kst = logical_date_utc.in_timezone(KST).strftime("%Y-%m-%d %H:%M:%S")
    run_time_utc_iso = logical_date_utc.isoformat()

    di_start = context.get("data_interval_start")
    di_end   = context.get("data_interval_end")
    di_kst = ""
    if di_start and di_end:
        di_kst = f"{di_start.in_timezone(KST).strftime('%Y-%m-%d %H:%M:%S')} ~ {di_end.in_timezone(KST).strftime('%Y-%m-%d %H:%M:%S')}"

    if ti is not None:
        start = ti.start_date
        end = ti.end_date or pendulum.now("UTC")
        owner = getattr(ti.task, "owner", "")
    else:
        start = getattr(dag_run, "start_date", None) if dag_run else None
        end = getattr(dag_run, "end_date", None) or pendulum.now("UTC") if dag_run else pendulum.now("UTC")
        owner = ""
    duration = _fmt_td((end - start).total_seconds() if start else 0)

    tags  = ", ".join(getattr(dag, "tags", []) or [])

    VM_EXTERNAL_IP = "34.50.49.51:8080"
    if ti is not None:
        log_url = ti.log_url
        if "localhost" in log_url:
            log_url = log_url.replace("localhost:8080", VM_EXTERNAL_IP)
    else:
        log_url = f"http://{VM_EXTERNAL_IP}/dags/{dag_id}/grid"

    main_blocks_1 = [
        {"type": "header", "text": {"type": "plain_text", "text": f":white_check_mark: DAG Succeeded - {dag_id}", "emoji": True}},
        {"type": "divider"},
    ]

    dag_field = {"type": "mrkdwn", "text": f"*DAG*\n`{dag_id}`"}
    second_field = (
        {"type": "mrkdwn", "text": f"*Task*\n`{ti.task_id}`"}
        if ti is not None
        else {"type": "mrkdwn", "text": f"*Duration*\n`{duration}`"}
    )
    main_blocks_2 = [
        {"type": "section", "fields": [dag_field, second_field]},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Run (KST)*\n`{run_time_kst}`"},
                {"type": "mrkdwn", "text": f"*Run (UTC)*\n`{run_time_utc_iso}`"},
            ]
        },
    ]
    if ti is not None:
        main_blocks_2.append({
            "type": "section",
            "fields": [{"type": "mrkdwn", "text": f"*Duration*\n`{duration}`"}]
        })

    extra_fields = []
    if owner: extra_fields.append({"type": "mrkdwn", "text": f"*Owner*\n{_owner_mention(context)}"})
    if tags:  extra_fields.append({"type": "mrkdwn", "text": f"*Tags*\n`{tags}`"})
    if extra_fields:
        main_blocks_2.append({"type": "section", "fields": extra_fields})

    button_block = [
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Logs  :airflow:", "emoji": True},
                    "url": log_url,
                }
            ]
        }
    ]

    footer_block = []
    if di_kst:
        footer_block = [
            {"type": "divider"},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": "Money Digger by Airflow"}]}
        ]

    payload = {
        "blocks": main_blocks_1,
        "attachments": [
            {"color": "#2EB67D", "blocks": main_blocks_2},
            {"blocks": button_block + footer_block},
        ],
        "unfurl_links": False,
    }

    try:
        SlackWebhookHook(slack_webhook_conn_id="slack_webhook_conn").send_dict(payload)
    except Exception as e:
        logging.exception("Slack callback failed: %s", e)
