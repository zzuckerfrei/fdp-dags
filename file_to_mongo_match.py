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
    dag_id="file_to_mongo_match",
    catchup=False,
    # schedule_interval="* * * * *",  # 5시간마다 실행 0시, 5시, 10시, 15시, 20시
    start_date=pendulum.datetime(2022, 9, 29, tz="UTC"),
)
def FileToMongoMatch():
    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    @task.python
    def get_file_path_match():
        res = fileToMongoItem.get_file_path("match")
        logging.info(f"get_file_path_match :: res is ... {res}")
        return {"res": res}  # xcom push

    @task.python
    def create_item_match(**context):
        xcom = context['task_instance'].xcom_pull(key="return_value", task_ids="get_file_path_match")
        logging.info("create_item_match :: xcom is ... {}".format(xcom))

        data_type = xcom["res"][0][0]
        file_path = xcom["res"][0][1]

        res = fileToMongoItem.create_item(data_type, file_path)
        logging.info(f"res ... {res}")
        return res

    @task.python
    def update_meta_match(**context):
        xcom = context['task_instance'].xcom_pull(key="return_value", task_ids="get_file_path_match")
        logging.info(f"create_item_match :: xcom is ... {xcom}")

        data_type = xcom["res"][0][0]
        file_path = xcom["res"][0][1]

        fileToMongoMeta.update_meta(data_type, file_path)

    start >> get_file_path_match() >> create_item_match() >> update_meta_match() >> end


dag = FileToMongoMatch()
