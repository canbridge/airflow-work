# coding=utf-8
import base64
import os
import datetime
import json
import sys
import zlib
import csv

from bson import ObjectId
from util import db, config

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


# fname = "/home/ubuntu/cfs-m/data/uncompress/track01"


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




def dirmove(filepath):
    cos_path =filepath
    if os.path.isdir(filepath):
        cos_path = cos_path + "/"
    cos_path = cos_path.replace("/home/ubuntu/cfs-m/data/uncompress", 'origin')
    cmd = "coscmd upload -r {}  {}".format(filepath, cos_path)
    os.system(cmd)


dag = DAG("move_origin",
          default_args={"owner": "ubuntu",
                        "start_date": airflow.utils.dates.days_ago(1)},
          schedule_interval=None,
          dagrun_timeout=datetime.timedelta(minutes=4)
          )


def _move(ds, **kwargs):
    fname = kwargs['dag_run'].conf['fname']
    # fname = "/home/ubuntu/cfs-m/data/uncompress/track01"
    # fname = "/home/ubuntu/cfs-m/data/uncompress/track01/100w前程/12月.rar_uncom/12月"
    # fname = "/home/ubuntu/cfs-m/data/uncompress/track01/50w精品简历库/新下载的简历/51job_alick_85611399_.mht"
    # fname = "/home/ubuntu/cfs-m/data/uncompress/track01/100w/智联无忧投递_4-28_20429份.zip_uncom/智联无忧投递/智联无忧投递3039.zip.uncom/智联无忧投递3039/智联投递938.zip.uncom/智联投递938/智联山东153.zip.uncom"
    dirmove(fname)


parse_resume = PythonOperator(
    task_id='_move',
    provide_context=True,
    python_callable=_move,
    dag=dag)
