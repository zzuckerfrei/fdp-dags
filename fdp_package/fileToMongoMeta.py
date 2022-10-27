import os
import requests
import logging
from airflow.models.variable import Variable


def delete_meta(data_type: str):
    try:
        url = Variable.get("url_meta_delete")
        params = {
            "data_type": data_type
        }
        res = requests.delete(url=f"{url}", params=params).json()

        logging.info(f"-----delete_meta success {data_type}-----")
        logging.info(f"res ... {res}")

    except Exception as e:
        raise Exception(e)
