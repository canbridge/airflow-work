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


def _trigger(path):
    dt = datetime.now().strftime("%Y-%m-%d_%H-%m")
    url = config.DAG_OPTIONS['url']
    run_id = "{}:{}".format(dt, path)
    data = {"api": "trigger_dag",
            "dag_id": "move_origin",
            # "run_id": run_id,
            }
    conf = {"fname": path}
    data["conf"] = json.dumps(conf)
    print("TRIGGER:", path, data)
    t = requests.get(url, params=data)
    print("RESULT:", t)


def dirlist(path, level = 0):
    if level == 3:
        dt = datetime.now().strftime("%Y-%m-%d_%H-%m")
        print(path, "LEVEL:", level)
        _trigger(path)
        return
    filelist =  os.listdir(path)
    for filename in filelist:
        filepath = os.path.join(path, filename)
        if os.path.isdir(filepath):
            dirlist(filepath, level = level + 1)
        else:
            _trigger(filepath)
            print(filepath, "LEVEL:", level)
            pass
            # print(filepath)

def _trigger_move(ds, **kwargs):
    f_dir = '/home/ubuntu/cfs-m/data/uncompress'
    for s in os.listdir(f_dir):
        if "track" in s:
            newDir = os.path.join(f_dir, s)
            print(newDir)
            dirlist(newDir)


dag = DAG("trigger_move",
          default_args={"owner": "ubuntu",
                        "start_date": airflow.utils.dates.days_ago(1)},
          schedule_interval=None,
          dagrun_timeout=timedelta(minutes=4)
          )

trigger_parse_dag = PythonOperator(
    task_id='_trigger_move',
    provide_context=True,
    python_callable=_trigger_move,
    dag=dag)
