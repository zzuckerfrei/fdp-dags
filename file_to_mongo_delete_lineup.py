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
    dag_id="file_to_mongo_delete_lineup",
    catchup=False,
    # schedule_interval="* * * * *",  # 5시간마다 실행 0시, 5시, 10시, 15시, 20시
    start_date=pendulum.datetime(2022, 9, 29, tz="UTC"),
)
def FileToMongoDeleteLineup():
    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    @task.python
    def delete_item():
        # mongo
        data_type_list = Variable.get("data_type_list", deserialize_json=True)
        res = fileToMongoItem.delete_item(data_type_list[2])  # 0 competition, 1 match, 2 lineup, 3 event
        logging.info(f"delete_item :: res is ... {res}")

        return {"res": res, "data_type": data_type_list[2]}  # xcom push

    @task.python
    def delete_meta(**context):
        # postgres
        xcom = context['task_instance'].xcom_pull(key="return_value", task_ids="delete_item")
        logging.info(f" xcom is ... {xcom}")
        res = fileToMongoMeta.delete_meta(data_type=xcom["data_type"])
        logging.info(f"delete_meta :: res is ... {res}")

        return {"res": res}  # xcom push

    start >> delete_item() >> delete_meta() >> end


dag = FileToMongoDeleteLineup()
