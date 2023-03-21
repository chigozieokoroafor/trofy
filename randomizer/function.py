import random, string
from pymongo.mongo_client import  MongoClient
from pymongo.collection import Collection

def createKey():
    a = random.choices(string.ascii_letters, k= 20)
    key = "tk_"+"".join(a)
    return key

class Sql:
    def __init__(self, connect_string) :
        self.connect_string = connect_string

class NoSql:
    def __init__(self, connect_string, dbName) :
        self.connect_string = connect_string
        self.dbName = dbName
    
    def connect(self):
        try:
            client_session = MongoClient(self.connect_string)
        except:
            client_session = None
        
        return client_session

    def getDatabase(self):
        try:
            connection = self.connect()
            db = connection.get_database(self.dbName)
            #collection.get_collection()
        except:
            return {"error":"couldn't connect to collection"}
        
        return db
    def getCollection(self, collection_name):
        db = self.getDatabase()
        col = db.get_collection(collection_name)
        if type(col) == Collection:
            return col, True
        else:
            return f"{collection_name} not found", False
    # def list_collections(self):
    #     connection =  self.connect()
    #     connection.get_database

