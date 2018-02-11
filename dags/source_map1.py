# coding=utf-8
import os
import pymysql
import json
import uuid
import requests
from datetime import timedelta, datetime
from sqlalchemy import text

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from util import db, config

file_path = '/home/ubuntu/airflow/dags/tmp/paths'
table = "resumes_to_parse_test"


def create_db_record(path):
    try:
        sql0 = 'select id from {} where path = "{}" limit 1'.format(table, path)
        mysql = db.DBSession()
        record = mysql.execute(text(sql0)).fetchall()
        if record:
            return
        sql = 'insert into {}(path) values("{}")'.format(table, path)
        mysql.execute(text(sql))
        mysql.commit()
    except Exception as e:
        mysql.rollback()
        raise


def get_files(file_dir, f):
    newDir = file_dir
    if os.path.isfile(file_dir):
        if "._txt" in file_dir:
            f.write(file_dir)
            f.write('\n')
    elif os.path.isdir(file_dir):
        for s in os.listdir(file_dir):
            newDir = os.path.join(file_dir, s)
            get_files(newDir, f)


def create_dag_job():
    mysql = db.DBSession()
    min_id = 1
    count = config.DAG_OPTIONS['parse']["count"]
    url = config.DAG_OPTIONS['url']
    while True:
        max_id = min_id + count
        sql = "select min(id), max(id) from {} where id >= {} and id <= {}".format(table, min_id, max_id)
        check = mysql.execute(text(sql)).fetchone()
        if check[0]:
            trigger_min_id, trigger_max_id = check
            dt = datetime.now().strftime("%Y-%m-%d_%H-%m")
            run_id = "{}-{}-{}".format(dt, trigger_min_id, trigger_max_id)
            data = {"api": "trigger_dag",
                    "dag_id": "parse",
                    "run_id": run_id,
                }
            conf = {"max_id": trigger_max_id,
                    "min_id": trigger_min_id,
                }
            data["conf"] = json.dumps(conf)
            requests.get(url, params=data)
            min_id = max_id + 1
        else:
            break



def _trigger_parse_dag(ds, **kwargs):
    create_dag_job()


def write_path_to_file(ds, **kwargs):
    file_dir = "/home/ubuntu/cfs-m/data/uncompress"
    f = open(file_path, "w")
    get_files(file_dir, f)
    f.close()


def write_path_to_db(ds, **kwargs):
    f = open(file_path, "r")
    tmp = f.readline().strip()
    while tmp:
        tmp = f.readline().strip()
        create_db_record(tmp)


dag = DAG("get_source_and_trigger_parse1",
          default_args={"owner": "ubuntu",
                        "start_date": airflow.utils.dates.days_ago(1)},
          schedule_interval=None,
          dagrun_timeout=timedelta(minutes=4)
          )

write_file = PythonOperator(
    task_id='write_file',
    provide_context=True,
    python_callable= write_path_to_file,
    dag=dag)


write_db = PythonOperator(
    task_id='write_db',
    provide_context=True,
    python_callable= write_path_to_db,
    dag=dag)


trigger_parse_dag = PythonOperator(
    task_id='trigger_parse_dag',
    provide_context=True,
    python_callable=_trigger_parse_dag,
    dag=dag)

trigger_parse_dag.set_upstream(write_db)
write_db.set_upstream(write_file)
