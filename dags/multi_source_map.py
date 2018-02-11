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
table = "resumes_to_parse_test"


class MyThread(Thread):
    def __init__(self, fdir, file_no):
        Thread.__init__(self)
        self.fdir = fdir
        self.file_no = file_no

    def run(self):
        fname = "/home/ubuntu/airflow/dags/tmp/{}".format(self.file_no)
        write_to_file(self.fdir, fname)


def write_to_file(fdir, fname):
    f = open(fname, 'w')
    get_files_osscan(fdir, f)
    f.close()


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


def get_files_osscan(file_dir, f):
    # print(ctypes.CDLL('libc.so.6').syscall(186))
    scan = os.scandir(file_dir)
    for s in scan:
        if s.is_dir():
            get_files_osscan(s.path, f)
        else:
            child = s.path
            if "._txt" in child:
                f.write(child)
                f.write('\n')


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
                    "run_id": run_id, }
            conf = {"max_id": trigger_max_id,
                    "min_id": trigger_min_id, }
            data["conf"] = json.dumps(conf)
            t = requests.get(url, params=data)
            print(t.text)
            min_id = max_id + 1
        else:
            break


def _trigger_parse_dag(ds, **kwargs):
    create_dag_job()


def write_path_to_file(ds, **kwargs):
    file_dir = "/home/ubuntu/cfs-m/data/uncompress"
    scan = os.scandir(file_dir)
    fileno = 1
    threads = []
    for entry in scan:
        if entry.is_dir():
            child_dir = entry.path
            write_name = "/home/ubuntu/airflow/dags/tmp/{}".format(fileno)
            t = Thread(target=write_to_file, args=(child_dir, write_name, ))
            t.start()
            threads.append(t)
            print(child_dir, t, fileno)
            fileno += 1

    for thread in threads:
        if thread.is_alive():
            thread.join()


def write_path_to_db(ds, **kwargs):
    scan = os.scandir(file_path)
    for entry in scan:
        if entry.is_dir():
            continue
        child_path = entry.path
        f = open(child_path, "r")
        tmp = f.readline().strip()
        while tmp:
            tmp = f.readline().strip()
            create_db_record(tmp)


dag = DAG("multi_get_source_and_trigger_parse",
          default_args={"owner": "ubuntu",
                        "start_date": airflow.utils.dates.days_ago(1)},
          schedule_interval=None,
          dagrun_timeout=timedelta(minutes=4)
          )

write_file = PythonOperator(
    task_id='write_file',
    provide_context=True,
    python_callable=write_path_to_file,
    dag=dag)


write_db = PythonOperator(
    task_id='write_db',
    provide_context=True,
    python_callable=write_path_to_db,
    dag=dag)


trigger_parse_dag = PythonOperator(
    task_id='trigger_parse_dag',
    provide_context=True,
    python_callable=_trigger_parse_dag,
    dag=dag)

trigger_parse_dag.set_upstream(write_db)
write_db.set_upstream(write_file)
