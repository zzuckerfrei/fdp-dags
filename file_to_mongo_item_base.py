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


@dag(
    dag_id="file_to_mongo_item_base",
    catchup=False,
    schedule_interval="* * * * *",  # every 1 min
    start_date=pendulum.datetime(2022, 11, 3, tz="UTC"),
    tags=["test", "base", "item"]
)
def FileToMongoItemBase():
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

            res = fileToMongoItem.get_file_path(data_type_list[3])  # 0 competition, 1 match, 2 lineup, 3 event
            logging.info(f"get_file_path_competition :: res is ... {res}")

            context['task_instance'].xcom_push(key='result', value=res)

        except Exception as e:
            context['task_instance'].xcom_push(key='status', value=1)
            raise AirflowException(e)

        else:
            context['task_instance'].xcom_push(key='status', value=0)

    @task.python(sla=datetime.timedelta(seconds=60))  # 아직 테스트는 안 함
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
    # start >> get_file_path_match() >> create_item_match() >> update_meta_match() >> end
    # start >> get_file_path_lineup() >> create_item_lineup() >> update_meta_lineup() >> end
    # start >> get_file_path_event() >> create_item_event() >> update_meta_event() >> end


dag = FileToMongoItemBase()
