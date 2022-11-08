import os
import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable
from airflow.exceptions import AirflowException


def get_file_path(data_type: str):
    postgres_hook = PostgresHook(postgres_conn_id="fdp_meta_pg_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    try:
        with open(Variable.get("sql_base_dir") + "item/get_file_path.sql", "r") as f:
            query = f.read()
        query = query.format(data_type="fdp_" + data_type)
        logging.info("query is ...{}".format(query))

        cur.execute(query)
        result = cur.fetchall()

        # 만약 한 건도 없을 경우 에러 발생
        if len(result[0]) == 0:
            raise FileNotFoundError("get_file_path :: NO DATA")

    except Exception as e:
        raise Exception(e)

    else:
        return result


def create_item(data_type: str, file_path: str):
    try:
        url = Variable.get("url_item_create")
        params = {
            "data_type": data_type,
            "org_name": file_path
        }
        # todo mongo 트랜잭션 추가하기
        # 복잡한 mongodb 트랜잭션 대신 한 번 select해서 존재여부 체크하고 없을 때만 적재 할까?
        res = requests.post(url=f"{url}", params=params).json()

        logging.info(f"-----create_item success {data_type}, {file_path}-----")
        logging.info(f"res ... {res}")

        return res

    except Exception as e:
        raise AirflowException(e)


def delete_item(data_type: str):
    try:
        url = Variable.get("url_item_delete")
        params = {
            "data_type": data_type,
        }
        res = requests.delete(url=f"{url}", params=params).json()

        logging.info(f"-----delete_item success {data_type}")
        logging.info(f"res ... {res}")

        return res

    except Exception as e:
        raise AirflowException(e)
