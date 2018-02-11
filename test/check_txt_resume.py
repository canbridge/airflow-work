from  pymongo  import MongoClient
import os
import random
import pymongo

uri = "mongodb://developer:Threfo0998@10.0.0.90/btp_staging?authSource=admin"
mc = MongoClient(uri)
data = mc['data']

coll = data["mongo_resume_table"]
print(coll.count())
print(dir(coll))
print(coll.dataSize())
