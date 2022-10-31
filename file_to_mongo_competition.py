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
    dag_id="file_to_mongo_competition",
    catchup=False,
    schedule_interval="*/30 * * * *",  # every 30 min
    start_date=pendulum.datetime(2022, 9, 29, tz="UTC"),
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
    def get_file_path_competition():
        data_type_list = Variable.get("data_type_list", deserialize_json=True)

        res = fileToMongoItem.get_file_path(data_type_list[0])
        logging.info(f"get_file_path_competition :: res is ... {res}")

        return {"res": res}  # xcom push

    @task.python
    def create_item_competition(**context):
        xcom = context['task_instance'].xcom_pull(key="return_value", task_ids="get_file_path_competition")
        logging.info("create_item_competition :: xcom is ... {}".format(xcom))

        data_type = xcom["res"][0][0]
        file_path = xcom["res"][0][1]

        res = fileToMongoItem.create_item(data_type, file_path)
        logging.info(f"res ... {res}")
        return res

    @task.python
    def update_meta_competition(**context):
        xcom = context['task_instance'].xcom_pull(key="return_value", task_ids="get_file_path_competition")
        logging.info(f"update_meta_competition :: xcom is ... {xcom}")

        data_type = xcom["res"][0][0]
        file_path = xcom["res"][0][1]

        fileToMongoMeta.update_meta(data_type, file_path)

    start >> get_file_path_competition() >> create_item_competition() >> update_meta_competition() >> end


dag = FileToMongoCompetition()
