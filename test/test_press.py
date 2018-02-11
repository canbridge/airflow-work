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

serverAddress = "10.0.0.115"

def content_parse(content):
    data = {
        'base_cont': content,
        'fname': "test.txt"
    }
    print(data)
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


fname = '/home/ubuntu/data/raw2/part-r-00037-d13cfcb7-8d60-45c1-b4a4-657a6a7b3217.csv'
with open(fname, 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        content = zlib.decompress(base64.b64decode(row[1]))
        content = base64.b64encode(content).decode('ascii')

        parsed = content_parse(content)
        break
        # resume = parsed.get("result", {})
        # print(resume)
