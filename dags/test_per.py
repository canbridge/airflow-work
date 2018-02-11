# coding=utf-8

'''
airflow trigger_dag --conf '{"min_id": "20", "max_id": "30"}' parser
'''

import datetime
import requests
import json
import base64
import sys

from bson import ObjectId


from threading import Thread, current_thread

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from util import db, config


def request_parse(name):
    serverAddress = "10.0.0.53"
    with open(name, 'rb') as reader:
        content = reader.read()
        content = base64.b64encode(content).decode('ascii')
        data = {
            'base_cont': content,
            'fname': "test.txt"
        }
        response = requests.post(
            'http://%s:2015/api/ResumeParser' % serverAddress,
            data=json.dumps(data),
            auth=('admin', '2015')
        )
        print(content, type(content))
        print(response, name, current_thread().getName())
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            return {}

# name = "/home/ubuntu/cfs-m/data/uncompress/track01/100w/智联无忧投递_4-28_20429份.zip_uncom/智联无忧投递/无忧智联投递11278.zip.uncom/无忧智联投递11278/智联投递2292.zip.uncom/智联投递2292/智联上海30.zip.uncom/14-03-21/上海/JM179880238R90250001000.html._txt"
# t = request_parse(name)
