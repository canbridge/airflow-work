import csv
import sys
import os
import re

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

doc_count = {}

count = 0
f_dir = '/home/ubuntu/raw1/raw1'
for s in os.listdir(f_dir):
    newDir = os.path.join(f_dir, s)
    fname = newDir
    #fname = '/home/ubuntu/raw1/raw1/track02-6164.csv'
    print ('FNAME:', fname)
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            d_id = row[0]
            pdf_path = row[2]
            t = re.search("\.[^\.]*$", pdf_path)
            doc = t.group()
            doc_count[doc] = doc_count.get(doc, 0) + 1
            target_d = "/home/ubuntu/airflow/dags/docx"
            if '.docx' in pdf_path:
                count += 1
                pdf_path = '/home/ubuntu/cfs-m/data/uncompress/' + pdf_path
                cmd = "cp  {}  {}".format(pdf_path, target_d)
                print(cmd)
                os.system(cmd)
                if count == 2000:
                    print("done", pdf_path)
                    break
print("result:", doc_count)
