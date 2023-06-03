import random, string, pymysql.cursors
from pymongo.mongo_client import  MongoClient
from pymongo.collection import Collection
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError
# import mysql.connector as ms_conn
# from mysql.connector import ProgrammingError
import json, psycopg2

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

class PostgresqlType:
    def __init__(self, connect_string):
        self.connect_string = connect_string
        

    def connect(self):
        try:
            makeConnection = psycopg2.connect(self.connect_string)
            return makeConnection, True
        except OperationalError as e:
            return f"Connection string provided incorrect. Either the database name specified doesn't exist or your connection string is invalid", False
    
    #check for groupTable specified
    #check for product table specified
    def tableCheck(self, table):
        conn = self.connect()
        if conn[1] == True:
            cursor = conn[0].cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
            tableList = cursor.fetchall()
            # cursor.close()
            # self.connect()[0].commit()

            # this checks if the table specified exists
            exists = False
            for i in tableList:
                if table in i:
                    exists = True
            return exists
        else:
            return False

    def dataFetch(self, table, key= "*", key_value=None, tag=None):
        cursor = self.connect()[0].cursor()
        query = f"""SELECT {key} FROM "{table}" """

        if tag == "items":
            query = f"""SELECT * FROM "{table}" WHERE {key}=={key_value} """
        cursor.execute(query)
        data = cursor.fetchall()
        if len(data)>0:
            return data, True
        else: 
            return data, False
    
    

        


# cursor.close()
# connection.commit()
        
        

    # fetch data from tables specified
        
    
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
    

