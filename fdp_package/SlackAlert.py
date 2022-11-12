from datetime import tzinfo
import pytz
from typing import List
from airflow.models.dag import DagContext
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

KST = pytz.timezone('Asia/Seoul')


def fail_alert(context) -> None:
    message = """:x: DAG fail alert\n\n"""
    completed = True

    # dag_run 객체 가져오기
    dag_instance = context["dag_run"]  # DagRun(airflow.models.dagrun.py)
    dag_id = dag_instance.dag_id
    message += f"- dag_id :: {dag_id}\n\n"

    # 해당 dag에 속한 task instances 목록을 가져옴
    task_instances = dag_instance.get_task_instances()
    for task in task_instances:
        # 각 task instance의 id와 state를 확인
        task_id = task.task_id
        state = task.current_state()
        if state == "failed":
            completed = False
        log_url = task.log_url
        message += f"- task_id :: {task_id},   state :: {state}\n" \
                   f"\t- log_url : <{log_url}|link>\n"

    execution_date = dag_instance.execution_date
    execution_date_kst = execution_date.replace(tzinfo=KST)  # KST (이 부분이 안먹음)
    message += f"- execution_date :: {execution_date} (KST : {execution_date_kst}\n"

    alert = SlackWebhookOperator(
        task_id='slack_fail',
        http_conn_id="slack_webhook",
        message=message
    )

    return alert.execute(context=context)
