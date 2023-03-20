import random, string
from pymongo.mongo_client import  MongoClient

def createKey():
    a = random.choices(string.ascii_letters, k= 20)
    key = "tk_"+"".join(a)
    return key

class Sql:
    def __init__(self, connect_string) :
        self.connect_string = connect_string

class NoSql:
    def __init__(self, connect_string) :
        self.connect_string = connect_string
    
    def connect(self):
        try:
            client_session = MongoClient(self.connect_string)
        except:
            client_session = None
        
        return client_session

    def db_collection(self, collection_name):
        try:
            connection = self.connect()
            collection = connection.get_database(collection_name)
        except:
            return {"error":"couldn't connect to collection"}
        print(collection.list_collection_names())
        return collection
    # def list_collections(self):
    #     connection =  self.connect()
    #     connection.get_database

