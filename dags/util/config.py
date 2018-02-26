from sqlalchemy import engine_from_config
from sqlalchemy.orm import sessionmaker, scoped_session

from  pymongo  import MongoClient


DB_OPTIONS = {
    'url': 'mysql://root:User@123@10.0.0.43:3306/resume?charset=utf8mb4',
    'pool_recycle': 3600,
    'echo': False,
}

MONGO_OPTIONS = {
    "uri": "mongodb://developer:Threfo0998@10.0.0.90/btp_staging?authSource=admin"
}

DAG_OPTIONS = {
    "url": "http://10.0.30.6:8080/admin/rest_api/api",
    "parse": {"count": 50000},
    "mysql_resume_path_table": 'resumes_path',

    "mongo_resume_tmp_table": "mongo_resume_tmp_table",
    "mongo_resume_table": "mongo_resume_table",
    "mongo_db": "data",

}

PARSER_OPTIONS = {
    'address': "10.0.30.37",
}
