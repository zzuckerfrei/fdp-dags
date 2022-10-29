import logging

from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


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