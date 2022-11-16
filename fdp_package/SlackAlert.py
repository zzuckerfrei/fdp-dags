import pytz

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

KST = pytz.timezone('Asia/Seoul')


def fail_alert(context) -> None:
    message = """:x: fail alert\n\n"""
    # completed = True

    # dag_run 객체 가져오기
    dag_instance = context["dag_run"]  # DagRun(airflow.models.dagrun.py)
    dag_id = dag_instance.dag_id
    message += f"*Dag* : {dag_id}\n\n"

    # 해당 dag에 속한 task instances 목록을 가져옴
    task_instances = dag_instance.get_task_instances()
    for task in task_instances:
        # 각 task 정보 확인
        task_id = task.task_id
        start_date = task.start_date
        end_date = task.end_date
        duration = task.duration
        state = task.current_state()
        operator = task.operator

        if state in ("failed", "running"):
            # completed = False
            log_url = task.log_url
            message += f"*Task* : {task_id}     /    *State* : {state}\n" \
                       f"*Log*   : <{log_url}|URL>\n" \
                       f"*operator* : {operator}\n"
            execution_date = dag_instance.execution_date
            execution_date_kst = KST.normalize(execution_date.astimezone(KST))
            message += f"*start_date* : {start_date}\n" \
                       f"*end_date* : {end_date}\n" \
                       f"*duration* : {duration}\n" \
                       f"*execution_date* : {execution_date}\n" \
                       f"*execution_date_KST* : {execution_date_kst}\n"

    alert = SlackWebhookOperator(
        task_id='slack_fail',
        http_conn_id="slack_webhook",
        message=message
    )

    return alert.execute(context=context)
