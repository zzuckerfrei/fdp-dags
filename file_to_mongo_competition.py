import os
import datetime
import logging
import requests
from time import sleep
import pendulum
import random

import psycopg2.extras
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from airflow.exceptions import AirflowTaskTimeout
from airflow.exceptions import AirflowException

from fdp_package import fileToMongoItem
from fdp_package import fileToMongoMeta
from fdp_package import SlackAlert


default_args = {
   'on_failure_callback': SlackAlert.fail_alert  # dag 실행 중 실패할 경우 호출하는 함수
}


@dag(
    dag_id="file_to_mongo_competition",
    catchup=True,
    schedule_interval="*/30 * * * *",  # every 30 min
    start_date=pendulum.datetime(2022, 11, 22, tz="UTC"),
    max_active_runs=1,
    default_args=default_args,
    tags=["file_to_mongo", "item", "competition"]
)
def FileToMongoCompetition():
    """
    mongoDB의 fdp.competition collection에 document 적재
    """
    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    @task.python
    def get_file_path_competition(**context):
        try:
            data_type_list = Variable.get("data_type_list", deserialize_json=True)

            res = fileToMongoItem.get_file_path(data_type_list[0])  # 0 competition, 1 match, 2 lineup, 3 event
            logging.info(f"get_file_path_competition :: res is ... {res}")

            context['task_instance'].xcom_push(key='result', value=res)

        except Exception as e:
            context['task_instance'].xcom_push(key='status', value=1)
            raise AirflowException(e)

        else:
            context['task_instance'].xcom_push(key='status', value=0)

    @task.python
    def create_item_competition(**context):
        try:
            status = context['task_instance'].xcom_pull(key="status", task_ids="get_file_path_competition")
            if status == 1:
                raise Exception("previous task status is 1")

            xcom = context['task_instance'].xcom_pull(key="result", task_ids="get_file_path_competition")
            logging.info("create_item_competition :: xcom is ... {}".format(xcom))

            data_type = xcom[0][0]
            file_path = xcom[0][1]

            res = fileToMongoItem.create_item(data_type, file_path)
            logging.info(f"res ... {res}")

            context['task_instance'].xcom_push(key='result', value=res)

        except Exception as e:
            context['task_instance'].xcom_push(key='status', value=1)
            raise AirflowException(e)

        else:
            context['task_instance'].xcom_push(key='status', value=0)

    @task.python
    def update_meta_competition(**context):
        try:
            status = context['task_instance'].xcom_pull(key="status", task_ids="create_item_competition")
            if status == 1:
                raise Exception("previous task status is 1")

            xcom = context['task_instance'].xcom_pull(key="result", task_ids="get_file_path_competition")
            logging.info(f"create_item_competition :: xcom is ... {xcom}")

            data_type = xcom[0][0]
            file_path = xcom[0][1]

            fileToMongoMeta.update_meta(data_type, file_path)

        except Exception as e:
            context['task_instance'].xcom_push(key='status', value=1)
            raise AirflowException(e)

        else:
            context['task_instance'].xcom_push(key='status', value=0)

    start >> get_file_path_competition() >> create_item_competition() >> update_meta_competition() >> end


dag = FileToMongoCompetition()
