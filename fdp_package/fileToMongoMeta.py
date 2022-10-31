import requests
import logging

import psycopg2.extras
from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_meta_one(url: str, data_type: str):
    try:
        res = requests.get(f"{url}/{data_type}").json()  # test (o), 172.18.0.3(o)

        logging.info("get meta one ... {}".format(res))
        # logging.info("date_type ... {}".format(res["result"]["data_type"]))  ## date_type
        # logging.info("count_in_dir ... {}".format(res["result"]["count_in_dir"]))  ## count_in_dir
        # logging.info("list_in_dir ... {}".format(res["result"]["list_in_dir"]))  ## list_in_dir

        return res

    except Exception as e:
        raise Exception(e)


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
        raise Exception(e)


def update_meta(data_type: str, file_path: str):
    try:
        # fdp_meta update
        with open(Variable.get("sql_base_dir") + "meta/update_table_meta.sql", "r") as f:
            query = f.read()
        query_meta = query.format(data_type=data_type)
        logging.info("query_meta is ...{}".format(query_meta))

        postgres_hook = PostgresHook(postgres_conn_id="fdp_meta_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query_meta)

        # fdp_item update
        with open(Variable.get("sql_base_dir") + "item/update_table_item.sql", "r") as f:
            query = f.read()
        query_item = query.format(data_type=data_type, file_path=file_path)
        logging.info("query_item is ...{}".format(query_item))
        cur.execute(query_item)

        conn.commit()

    except Exception as e:
        raise Exception(e)


def delete_meta(data_type: str):
    try:
        # fdp_meta delete, fdp_item delete
        with open(Variable.get("sql_base_dir") + "meta/delete_meta.sql", "r") as f:
            query = f.read()
        query_meta = query.format(data_type=data_type)
        logging.info("query is ...{}".format(query_meta))

        postgres_hook = PostgresHook(postgres_conn_id="fdp_meta_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query_meta)

        conn.commit()

    except Exception as e:
        raise Exception(e)
