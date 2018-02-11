from sqlalchemy import engine_from_config
from sqlalchemy.orm import sessionmaker, scoped_session
from  pymongo  import MongoClient

from  .config import DB_OPTIONS, MONGO_OPTIONS

sa_engine = engine_from_config(DB_OPTIONS, prefix="")
DBSession = scoped_session(sessionmaker(bind=sa_engine))

mongo_client = MongoClient(MONGO_OPTIONS['uri'])
