# coding=utf-8

'''
airflow trigger_dag --conf '{"min_id": "20", "max_id": "30"}' parser
'''

import datetime
import requests
import json
import base64
import sys
import zlib
import csv

from bson import ObjectId


from threading import Thread, current_thread

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text

mongo_client = MongoClient(MONGO_OPTIONS['uri'])

maxInt = sys.maxsize
decrement = True

while decrement:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.
    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/10)
        decrement = True

serverAddress = "10.0.0.115"

dag = DAG("parse_raw2_resume_folder",
          default_args={"owner": "ubuntu",
                        "start_date": airflow.utils.dates.days_ago(1)},
          schedule_interval=None,
          dagrun_timeout=datetime.timedelta(minutes=4)
          )


def change_key(dict_):
    keys = dict_.keys()
    for key in keys:
        if "$" in key:
            new_key = key.replace("$", '#')
            dict_[new_key] = dict_[key]
            del dict_[key]


def content_parse(content):
    data = {
        'base_cont': content,
        'fname': "test.txt"
    }
    response = requests.post(
        'http://%s:2015/api/ResumeParser' % serverAddress,
        data=json.dumps(data),
        auth=('admin', '2015')
    )
    if response.status_code == 200:
        result = response.json()
        return result
    else:
        return {}


def itera_parse(fname):
    mongodb = db.mongo_client.data
    coll = mongodb.mongo_resume_tmp_table
    serverAddress = "10.0.0.115"

    coll.delete_many({"fname": fname})

    with open(fname, 'r') as csvfile:
        resumes = []
        reader = csv.reader(csvfile)
        for row in reader:
            content = zlib.decompress(base64.b64decode(row[1]))
            content = base64.b64encode(content).decode('ascii')

            parsed = content_parse(content)
            resume = parsed.get("result", {})
            change_key(resume)

            resume['fname'] = fname

            resumes.append(resume)

            if len(resumes) == 60:
                coll.insert_many(resumes)
                resumes = []

        if resumes:
            coll.insert_many(resume)


def _parse(ds, **kwargs):
    fname = kwargs['dag_run'].conf['fname']
    # fname = "/home/ubuntu/data/raw2/part-r-00000-d13cfcb7-8d60-45c1-b4a4-657a6a7b3217.csv"
    itera_parse(fname)


parse_resume = PythonOperator(
    task_id='_parse_resume',
    provide_context=True,
    python_callable=_parse,
    dag=dag)
