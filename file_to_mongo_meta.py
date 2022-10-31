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

from fdp_package import fileToMongoMeta

@dag(
    dag_id="file_to_mongo_meta",
    catchup=False,
    schedule_interval="0 * * * *",  # 1 day, 0:00:00
    start_date=pendulum.datetime(2022, 9, 29, tz="UTC"),
    tags=["file_to_mongo", "first_execute", "meta"]
)
def FileToMongo():
    """
    가장 먼저 실행되어야 하는 dag \n
    메타 데이터(meta스키마의 모든 테이블)를 적재 \n
    """
    start = EmptyOperator(
        task_id='start'
    )

    check_schema_exists = PostgresOperator(
        task_id="create_schema_meta",
        postgres_conn_id="fdp_meta_pg_conn",
        sql="sql/meta/create_schema_meta.sql",
    )

    create_table_meta_meta = PostgresOperator(
        task_id="create_table_meta_meta",
        postgres_conn_id="fdp_meta_pg_conn",
        sql="sql/meta/create_table_meta_meta.sql",
    )

    create_table_meta_competition = PostgresOperator(
        task_id="create_table_meta_competition",
        postgres_conn_id="fdp_meta_pg_conn",
        sql="sql/meta/create_table_meta_competition.sql",
    )

    create_table_meta_match = PostgresOperator(
        task_id="create_table_meta_match",
        postgres_conn_id="fdp_meta_pg_conn",
        sql="sql/meta/create_table_meta_match.sql",
    )

    create_table_meta_lineup = PostgresOperator(
        task_id="create_table_meta_lineup",
        postgres_conn_id="fdp_meta_pg_conn",
        sql="sql/meta/create_table_meta_lineup.sql",
    )

    create_table_meta_event = PostgresOperator(
        task_id="create_table_meta_event",
        postgres_conn_id="fdp_meta_pg_conn",
        sql="sql/meta/create_table_meta_event.sql",
    )

    @task.python
    def get_meta_competition():
        try:
            url = Variable.get("url_meta_create")  # http://172.18.0.3:8000/api/v1/meta/read_one
            data_type_list = Variable.get("data_type_list",
                                          deserialize_json=True)  # ["competition", "match", "lineup", "event"]
            res = fileToMongoMeta.get_meta_one(url, data_type_list[0])
            fileToMongoMeta.insert_meta_one(res, data_type_list[0])

        except Exception as e:
            raise AirflowException(e)

    @task.python
    def get_meta_match():
        try:
            url = Variable.get("url_meta_create")
            data_type_list = Variable.get("data_type_list", deserialize_json=True)
            res = fileToMongoMeta.get_meta_one(url, data_type_list[1])
            fileToMongoMeta.insert_meta_one(res, data_type_list[1])

        except Exception as e:
            raise AirflowException(e)

    @task.python
    def get_meta_lineup():
        try:
            url = Variable.get("url_meta_create")
            data_type_list = Variable.get("data_type_list", deserialize_json=True)
            res = fileToMongoMeta.get_meta_one(url, data_type_list[2])
            fileToMongoMeta.insert_meta_one(res, data_type_list[2])

        except Exception as e:
            raise AirflowException(e)

    @task.python
    def get_meta_event():
        try:
            url = Variable.get("url_meta_create")
            data_type_list = Variable.get("data_type_list", deserialize_json=True)
            res = fileToMongoMeta.get_meta_one(url, data_type_list[3])
            fileToMongoMeta.insert_meta_one(res, data_type_list[3])

        except Exception as e:
            raise AirflowException(e)

    end = EmptyOperator(
        task_id='end'
    )

    start >> check_schema_exists >> create_table_meta_meta >> [create_table_meta_competition, create_table_meta_match,
                                                               create_table_meta_lineup, create_table_meta_event]

    create_table_meta_competition >> get_meta_competition() >> end
    create_table_meta_match >> get_meta_match() >> end
    create_table_meta_lineup >> get_meta_lineup() >> end
    create_table_meta_event >> get_meta_event() >> end


dag = FileToMongo()


