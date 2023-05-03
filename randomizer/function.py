import random, string, pymysql.cursors
from pymongo.mongo_client import  MongoClient
from pymongo.collection import Collection
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError
# import mysql.connector as ms_conn
# from mysql.connector import ProgrammingError
import json

def createKey():
    a = random.choices(string.ascii_letters, k= 20)
    key = "tk_"+"".join(a)
    return key

class SQLType:
    def __init__(self, connect_string, tableName) :
        self.connect_string = connect_string
        self.tableName = tableName
    
    def connect(self):
        try:
            engine = create_engine(url=self.connect_string)
            conn = engine.raw_connection()
            tables_list = inspect(engine).get_table_names()
            if self.tableName in tables_list:
                return conn, True
            else:
                return f"Table, {self.tableName}, doesn't exist, provide an existing table", False
                
        except OperationalError as E:
            return "Connection string provided is invalid, provide valid connection string ", False

    def getTableData(self, primaryKey):
        connection = self.connect()
        cursor = connection[0].cursor(pymysql.cursors.DictCursor) 
        query = f"SELECT {primaryKey} FROM {self.tableName}"
        try:
            ex= cursor.execute(query)
            if type(ex) == int and ex > 0:
                return cursor.fetchall(),True
            else:
                return {"detail":f"data using primaryKey {primaryKey} not found, provide valid primaryKey", "success":False}
        except Exception as e:
            return {"detail":str(e), "success":False}
    
    def getProductData(self):
        engine = create_engine(url=self.connect_string)
        connection = engine.raw_connection()
        
        cursor = connection.cursor(pymysql.cursors.DictCursor) 
        query = f"SELECT * FROM {self.tableName}"
        try:
                ex= cursor.execute(query)
                if type(ex) == int and ex > 0:
                    
                    return cursor.fetchall(),True
                else:
                    return f"data not found, provide valid primaryKey", False
        except Exception as e:
                
                return str(e), False
        # else:
            # print(f"couldn't connect to {self.connect_string}")
    
    def getGroupData(self):
        connection = self.connect()
        cursor = connection[0].cursor(pymysql.cursors.DictCursor) 
        query = f"SELECT * FROM {self.tableName}"
        try:
            ex= cursor.execute(query)
            if type(ex) == int and ex > 0:
                return cursor.fetchall(),True
            else:
                return "Error accored", False 
        except Exception as e:
            return {"detail":str(e), "success":False}
        

# either let all the nosql type db fall under  a NoSQl Class 
# or make each of them seperate classes of their own. 

class Mongodb:
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
    

