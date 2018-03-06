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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from util import db, config

file_path = '/home/ubuntu/airflow/dags/tmp'
table = config.DAG_OPTIONS['mysql_resume_path_table']


def create_dag_job():
    f_dir = '/home/ubuntu/raw1/raw1'
    url = config.DAG_OPTIONS['url']
    for s in os.listdir(f_dir):
        newDir = os.path.join(f_dir, s)
        dt = datetime.now().strftime("%Y-%m-%d_%H-%m")
        print(newDir)
        run_id = "{}:{}".format(dt, newDir)
        data = {"api": "trigger_dag",
                "dag_id": "parse_raw1_resume",
                "run_id": run_id, }
        conf = {"fname": newDir}
        data["conf"] = json.dumps(conf)
        t = requests.get(url, params=data)


def _trigger_parse_dag(ds, **kwargs):
    create_dag_job()


dag = DAG("trigger_get_raw1_pdf",
          default_args={"owner": "ubuntu",
                        "start_date": airflow.utils.dates.days_ago(1)},
          schedule_interval=None,
          dagrun_timeout=timedelta(minutes=4)
          )

trigger_parse_dag = PythonOperator(
    task_id='_trigger_parse',
    provide_context=True,
    python_callable=_trigger_parse_dag,
    dag=dag)
