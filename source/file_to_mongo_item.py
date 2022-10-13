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


@dag(
    dag_id="file_to_mongo_meta",
    catchup=False,
    # schedule_interval="0 * * * *",  # 5시간마다 실행 0시, 5시, 10시, 15시, 20시
    start_date=pendulum.datetime(2022, 9, 29, tz="UTC"),
)
def FileToMongoItem():
    start = EmptyOperator(
        task_id='start'
    )

    end = EmptyOperator(
        task_id='end'
    )

    @task.python
    def get_file_path_competition():
        res = get_file_path("competition")
        pass

    @task.python
    def create_item_competition():
        create_item()
        pass

    start >> get_file_path_competition() >> create_item_competition() >> update_meta_competition() >> end
    start >> get_file_path_match() >> create_item_match() >> update_meta_match() >> end
    start >> get_file_path_lineup() >> create_item_lineup() >> update_meta_lineup() >> end
    start >> get_file_path_event() >> create_item_event() >> update_meta_event() >> end


dag = FileToMongoItem()




# 쿼리 실행 -> data_type, file_path 얻기
"""
  SELECT DATA_TYPE, FILE_PATH
    FROM META.FDP_LINEUP
   WHERE MONGO_FLAG = 'N'
ORDER BY FILE_PATH ASC
"""


def get_file_path(data_type: str):
    try:
        with open(os.path.dirname(os.path.realpath(__file__)) + "/sql/item/get_file_path.sql", "r") as f:
            query = f.read()
        query.format(data_type=data_type)
        logging.info("query is ...{}".format(query))

        postgres_hook = PostgresHook(postgres_conn_id="fdp_meta_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()

        return result

    except Exception as e:
        raise AirflowException(e)


# api호출 -> mongo에 적재
def create_item(data_type: str, file_path: str):
    try:
        url = Variable.get("url_item_create")
        params = {
            "data_type": data_type,
            "org_name": file_path
        }
        res = requests.get(url=f"{url}", params=params).json()

        logging.info(f"-----create_item success {data_type}, {file_path}-----")

    except Exception as e:
        raise AirflowException(e)


# 성공시 meta(count_in_db), 각item(mongo_flag) 업데이트
def update_meta():
    try:
        pass
    except Exception as e:
        raise AirflowException(e)
