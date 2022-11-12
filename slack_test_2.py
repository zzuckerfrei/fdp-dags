import pendulum

from fdp_package import SlackAlert

from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


default_args = {
   'on_failure_callback': SlackAlert.fail_alert
}


@dag(
    dag_id="slack_test_2",
    start_date=days_ago(0),
    max_active_runs=1,
    catchup=False,
    schedule_interval="@once",
    default_args=default_args,
)
def SlackTest2():
    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    @task.python
    def fail(**context):
        try:
            res = 1 / 0

        except Exception as e:
            context['task_instance'].xcom_push(key='status', value=1)
            raise Exception(e)

        else:
            context['task_instance'].xcom_push(key='status', value=0)

    start >> fail() >> end


dag = SlackTest2()
