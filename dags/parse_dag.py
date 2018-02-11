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


table = config.DAG_OPTIONS['mysql_resume_path_table']
# table = "resumes_to_parse_test"

dag = DAG("parse_resume",
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


def request_parse(name):
    serverAddress = "10.0.0.115"
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
        print(response, name, current_thread().getName())
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            # print('Error response. please check')
            return {}


def itera_parse(results):
    # mongodb = db.mongo_client[config.DAG_OPTIONS['mongo_db']]
    mongodb = db.mongo_client.data
    # coll = mongodb[config.DAG_OPTIONS['mongo_resume_tmp_table']]
    coll = mongodb.mongo_resume_tmp_table
    resumes = []
    for i in results:
        fname = i[1]
        id_ = i[0]
        dat = {"id": id_, "url": fname}

        reqd1 = datetime.datetime.now()
        parsed = request_parse(fname)
        resume = parsed.get("result", {})
        change_key(resume)
        reqd2 = datetime.datetime.now()
        resume["id"] = id_

        resumes.append(resume)
        if len(resumes) == 400:
            coll.insert_many(resumes)
            resumes = []

        print(id_)
        # insd1 = datetime.datetime.now()
        # res = coll.find_one({"id": id_})
        # if res:
        #     coll.update({"id": id_}, {"$set": resume})
        # else:
        #     coll.insert_one(resume)
        # insd2 = datetime.datetime.now()
        # print(id_, "parse_post time{}s, {}微秒".format((reqd2-reqd1).seconds, (reqd2-reqd1).microseconds), \
        #       "insert {}s, {}weimiao".format((insd2-insd1).seconds, (insd2-insd1).microseconds), current_thread().getName())
        print(id_, "parse_post time{}s, {}微秒".format((reqd2-reqd1).seconds, (reqd2-reqd1).microseconds))

    if resumes:
        coll.insert_many(resume)


def _parse(ds, **kwargs):
    min_id = kwargs['dag_run'].conf['min_id']
    max_id = kwargs['dag_run'].conf['max_id']
    # max_id = 928018
    # min_id = 828018
    mysql = db.DBSession()
    sql = 'select id, path from {} where id >= {} and id <= {}'.format(table, min_id, max_id)
    results = mysql.execute(text(sql)).fetchall()
    itera_parse(results)

    # threads = []
    # def changes(a, b):
    #     for i in range(0, len(a), b):
    #         yield a[i:i+b]
    # for records in changes(results, 2000):
    #     print(records[0][0], "begin")
    #     t = Thread(target=itera_parse, args=(records, ))
    #     t.start()
    #     threads.append(t)
    # for i in threads:
    #     if i.is_alive():
    #         i.join()


parse_resume = PythonOperator(
    task_id='_parse_resume',
    provide_context=True,
    python_callable=_parse,
    dag=dag)
