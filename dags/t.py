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


def _trigger(path):
    run_id = "{}:{}".format(dt, path)
    data = {"api": "trigger_dag",
            "dag_id": "move_origin",
            "run_id": run_id, }
    conf = {"fname": path}
    data["conf"] = json.dumps(conf)
    t = requests.get(url, params=data)


def dirlist(path, level = 0):
    if level == 3:
        dt = datetime.now().strftime("%Y-%m-%d_%H-%m")
        print(path, "LEVEL:", level)
        # _trigger(path)
        return
    filelist =  os.listdir(path)
    for filename in filelist:
        filepath = os.path.join(path, filename)
        if os.path.isdir(filepath):
            dirlist(filepath, level = level + 1)
        else:
            # _trigger(filepath)
            print(filepath, "LEVEL:", level)
            pass
            # print(filepath)


if __name__ == "__main__":
    f_dir = '/home/ubuntu/cfs-m/data/uncompress'
    print(f_dir)
    for s in os.listdir(f_dir):
        if "track" in s:
            newDir = os.path.join(f_dir, s)
            print(newDir)
            dirlist(newDir)
