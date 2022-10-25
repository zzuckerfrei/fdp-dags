import os
import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable


def get_file_path(data_type: str):
    try:
        with open(Variable.get("sql_base_dir") + "item/get_file_path.sql", "r") as f:
            query = f.read()
        query = query.format(data_type="fdp_" + data_type)
        logging.info("query is ...{}".format(query))

        postgres_hook = PostgresHook(postgres_conn_id="fdp_meta_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()

        return result

    except Exception as e:
        raise Exception(e)


def create_item(data_type: str, file_path: str):
    try:
        url = Variable.get("url_item_create")
        params = {
            "data_type": data_type,
            "org_name": file_path
        }
        res = requests.post(url=f"{url}", params=params).json()

        logging.info(f"-----create_item success {data_type}, {file_path}-----")
        logging.info(f"res ... {res}")

        return res

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
