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
# table = "resumes_to_parse_test"


def create_dag_job():
    mysql = db.DBSession()
    count = config.DAG_OPTIONS['parse']["count"]
    url = config.DAG_OPTIONS['url']
    sql_min_max = "select min(id), max(id) from {}".format(table)
    rec_minid, rec_maxid = mysql.execute(text(sql_min_max)).fetchone()
    min_id = rec_minid
    while min_id < rec_maxid:
        max_id = min_id + count
        print("check", min_id, max_id)
        sql = "select min(id), max(id) from {} where id >= {} and id <= {}".format(table, min_id, max_id)
        check = mysql.execute(text(sql)).fetchone()
        if check[0]:
            trigger_min_id, trigger_max_id = check
            dt = datetime.now().strftime("%Y-%m-%d_%H-%m")
            run_id = "{}-{}-{}".format(dt, trigger_min_id, trigger_max_id)
            data = {"api": "trigger_dag",
                    "dag_id": "parse_resume",
                    "run_id": run_id, }
            conf = {"max_id": trigger_max_id,
                    "min_id": trigger_min_id, }
            data["conf"] = json.dumps(conf)
            print("request", trigger_min_id, trigger_max_id)
            t = requests.get(url, params=data)
        min_id = max_id + 1

def _trigger_parse_dag(ds, **kwargs):
    create_dag_job()



dag = DAG("trigger_parse_resume",
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
