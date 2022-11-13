# import datetime
# import logging
# import requests
# from time import sleep
# import pendulum
# import random
#
# from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.models.variable import Variable
# from airflow.exceptions import AirflowTaskTimeout
# from airflow.exceptions import AirflowException
#
#
# @dag(
#     dag_id="test_file_to_mongo",
#     catchup=False,
#     # schedule_interval="0 * * * *",  # 5시간마다 실행 0시, 5시, 10시, 15시, 20시
#     start_date=pendulum.datetime(2022, 9, 29, tz="UTC"),
# )
# def FileToMongo():
#     @task.python
#     def start():
#         return 0
#
#     @task.python
#     def get_meta():
#         try:
#             # a = requests.get("http://127.0.0.1:8080/home")
#             # logging.info("get a {}".format(a))
#
#             # b = requests.get("http://127.0.0.1:8000/api/v1/meta/read_all/")
#             b = requests.get("http://172.18.0.3:8000/api/v1/meta/read_all/").json()  # test (o), 172.18.0.3(o)
#             logging.info("get meta ... {}".format(b))
#             return 0
#
#         except Exception as e:
#             raise AirflowException(e)
#             # logging.error(e)
#             # return 1
#
#     start()
#     get_meta()
#
#
# dag = FileToMongo()
