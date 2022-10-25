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
            url = Variable.get("url_meta_read_one")  # http://172.18.0.3:8000/api/v1/meta/read_one
            data_type_list = Variable.get("data_type_list",
                                          deserialize_json=True)  # ["competition", "match", "lineup", "event"]
            res = get_meta_one(url, data_type_list[0])
            insert_meta_one(res, data_type_list[0])

        except Exception as e:
            raise AirflowException(e)

    @task.python
    def get_meta_match():
        try:
            url = Variable.get("url_meta_read_one")
            data_type_list = Variable.get("data_type_list", deserialize_json=True)
            res = get_meta_one(url, data_type_list[1])
            insert_meta_one(res, data_type_list[1])

        except Exception as e:
            raise AirflowException(e)

    @task.python
    def get_meta_lineup():
        try:
            url = Variable.get("url_meta_read_one")
            data_type_list = Variable.get("data_type_list", deserialize_json=True)
            res = get_meta_one(url, data_type_list[2])
            insert_meta_one(res, data_type_list[2])

        except Exception as e:
            raise AirflowException(e)

    @task.python
    def get_meta_event():
        try:
            url = Variable.get("url_meta_read_one")
            data_type_list = Variable.get("data_type_list", deserialize_json=True)
            res = get_meta_one(url, data_type_list[3])
            insert_meta_one(res, data_type_list[3])

        except Exception as e:
            raise AirflowException(e)

    start = EmptyOperator(
        task_id='start'
    )
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


def get_meta_one(url: str, data_type: str):
    try:
        res = requests.get(f"{url}/{data_type}").json()  # test (o), 172.18.0.3(o)

        # logging.info("get meta one ... {}".format(res))
        # logging.info("date_type ... {}".format(res["result"]["data_type"]))  ## date_type
        # logging.info("count_in_dir ... {}".format(res["result"]["count_in_dir"]))  ## count_in_dir
        # logging.info("list_in_dir ... {}".format(res["result"]["list_in_dir"]))  ## list_in_dir

        return res

    except Exception as e:
        raise AirflowException(e)


def insert_meta_one(res: dict, data_type: str):
    try:
        count_in_dir = res["result"]["count_in_dir"]
        list_in_dir = res["result"]["list_in_dir"]

        postgres_hook = PostgresHook(postgres_conn_id="fdp_meta_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # fdp_meta
        sql = "INSERT INTO meta.fdp_meta(data_type, count_in_dir, count_in_db, reg_date) VALUES('%s','%s','%s',%s)" % (
            data_type,
            count_in_dir,
            "0",
            "default"
        )
        cur.execute(sql)

        # fdp_item
        sql = f'INSERT INTO meta.fdp_{data_type} (data_type, file_path, mongo_flag) VALUES %s'
        argslist = [(data_type, i, "N") for i in list_in_dir]

        psycopg2.extras.execute_values(cur=cur, sql=sql, argslist=argslist)

        conn.commit()

        return 0

    except Exception as e:
        raise AirflowException(e)
