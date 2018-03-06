# coding=utf-8

'''
airflow trigger_dag --conf '{"min_id": "20", "max_id": "30"}' parser
'''

import datetime
import os
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

from util import db, config

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

serverAddress = config.PARSER_OPTIONS["address"]

dag = DAG("parse_raw1_resume",
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

def upload_to_cos(filepath):
    result = {}
    a = filepath.split('/')
    d = a[6:9]
    p = '__'.join(a[9:])
    kuozhan = p.split('.')[-1]
    cos_d = '/'.join(d)
    cos_d = 'origin_tmp/' + cos_d
    zp = zlib.compress(p.encode('utf-8'))
    t = base64.b64encode(zp)
    t = t.decode("utf-8")
    m = ''.join(t.split("/"))
    cos_path = cos_d + '/' + m + '.' + kuozhan
    cmd = "coscmd upload -r {}  {}".format(filepath, cos_path)
    os.system(cmd)
    print(cmd)
    return cos_path



def itera_parse(fname):
    mongodb = db.mongo_client.data
    coll = mongodb["pdf_orgin"]
    coll.remove({"fname": fname})

    count = 1
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:

            d_id = row[0]
            pdf_path = row[2]
            try:

                if '.pdf' in pdf_path:
                    resume = {}
                    pdf_path = '/home/ubuntu/cfs-m/data/uncompress/' + pdf_path
                    cos_path = upload_to_cos(pdf_path)
                    resume['fname'] = fname
                    resume['d_id'] = d_id
                    resume['cos'] = cos_path
                    resume['origin'] = pdf_path
                    coll.insert_one(resume)

            except Exception as e:
                print("ERROR", "FNAME:", fname, "ERROR:", e, "PDFPATH:", pdf_path)
            count += 1


def _parse(ds, **kwargs):
    fname = kwargs['dag_run'].conf['fname']
    # fname = '/home/ubuntu/raw1/track01-6095.csv'
    # fname = "/home/ubuntu/data/raw2/part-r-00000-d13cfcb7-8d60-45c1-b4a4-657a6a7b3217.csv"
    itera_parse(fname)


parse_resume = PythonOperator(
    task_id='_parse_resume',
    provide_context=True,
    python_callable=_parse,
    dag=dag)
