from pymongo.mongo_client import MongoClient

connect =  MongoClient()

db = connect["TROPY"]

users = db["conn_col"]