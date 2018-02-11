import datetime
import requests
import json
import base64
import sys
import zlib
import csv

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

count = 0
fname = '/home/ubuntu/data/raw2/part-r-00037-d13cfcb7-8d60-45c1-b4a4-657a6a7b3217.csv'
with open(fname, 'r') as csvfile:
    resumes = []
    reader = csv.reader(csvfile)
    for row in reader:
        print(len(row), count)
        count += 1
#         content = zlib.decompress(base64.b64decode(row[1]))
#         content = base64.b64encode(content).decode('ascii')
#
#         parsed = content_parse(content)
#         resume = parsed.get("result", {})
#         change_key(resume)
#
#         resumes.append(resume)
#
#         if len(resumes) == 60:
#             coll.insert_many(resumes)
#             resumes = []
#
#     if resumes:
#         coll.insert_many(resume)
