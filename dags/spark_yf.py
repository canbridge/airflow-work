# coding=utf-8
import os
import pymysql
import json
import uuid
from threading import Thread
import ctypes

import requests
from datetime import timedelta, datetime
from sqlalchemy import text

import airflow
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from util import db, config

file_path = '/home/ubuntu/airflow/dags/tmp'
table = config.DAG_OPTIONS['mysql_resume_path_table']


def create_dag_job():
    f_dir = '/home/ubuntu/data/raw2'
    url = config.DAG_OPTIONS['url']
    for s in os.listdir(f_dir):
        newDir = os.path.join(f_dir, s)
        dt = datetime.now().strftime("%Y-%m-%d_%H-%m")
        print(newDir)
        run_id = "{}:{}".format(dt, newDir)
        data = {"api": "trigger_dag",
                "dag_id": "parse_raw2_resume",
                "run_id": run_id, }
        conf = {"fname": newDir}
        data["conf"] = json.dumps(conf)
        t = requests.get(url, params=data)


def _trigger_parse_dag(ds, **kwargs):
    create_dag_job()


dag = DAG("spark_yf_to_target",
          default_args={"owner": "ubuntu",
                        "start_date": airflow.utils.dates.days_ago(1)},
          schedule_interval=None,
          dagrun_timeout=timedelta(minutes=4)
          )


spark_yf = SparkSubmitOperator(
    task_id='_trigger_parse',
    provide_context=True,
    py_files='/home/ubuntu/airflow/spark_scripts/spark2.py',
    conn_id='con',
    dag=dag)
