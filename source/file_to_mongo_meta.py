import datetime
import logging
import requests
from time import sleep
import pendulum
import random

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
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
def FileToMongo():

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
        task_id="create_table_meta_competiion",
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
    def get_meta():
        try:
            # a = requests.get("http://127.0.0.1:8080/home")
            # logging.info("get a {}".format(a))

            # b = requests.get("http://127.0.0.1:8000/api/v1/meta/read_all/")
            b = requests.get("http://172.18.0.3:8000/api/v1/meta/read_all/").json()  # test (o), 172.18.0.3(o)
            logging.info("get meta ... {}".format(b))
            return 0

        except Exception as e:
            raise AirflowException(e)
            # logging.error(e)
            # return 1

    check_schema_exists >> create_table_meta_meta >> create_table_meta_competition >> create_table_meta_match >> create_table_meta_lineup >> create_table_meta_event
    # get_meta()


dag = FileToMongo()
