import random, string, pymysql.cursors
from pymongo.mongo_client import  MongoClient
from pymongo.collection import Collection
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError
import smtplib
import datetime, jwt, ssl
from email.mime.text import MIMEText
import json, psycopg2
from flask import jsonify
from functools import wraps
support_mail = "okoroaforc14@gmail.com"
password = "ecmhllyxrchptmqo"
secret_key = "tvMNclzgFHdoqQZkjKnt"
def createKey():
    a = random.choices(string.ascii_letters, k= 20)
    key = "tk_"+"".join(a)
    return key

def convDict(keys, array_list):
    
    a = zip(keys, array_list)
    return a


class Authentication:
    def generate_access_token(data, minutes=60):
        exp = datetime.datetime.now() + datetime.timedelta(minutes=minutes)
        data["exp"] = datetime.datetime.timestamp(exp)
        token = jwt.encode(data, secret_key,algorithm="HS256")
        return token
    
    def generate_otp():
        otpcode = "".join(random.choices(string.digits, k=4))
        expiry = datetime.timedelta(minutes=5.0)
        start_time = datetime.datetime.timestamp(datetime.datetime.now()) 
        stop_time = datetime.datetime.timestamp(datetime.datetime.now() + expiry)
        return {"otp":otpcode, "stoptime":stop_time, "starttime":start_time}
    
    def mail_send(email, temp, mail_title):
        try:
            email_sender = support_mail
            email_password = password

            email_reciever = email
            #subject = "test"
            #file = open("other/verification.html")
            # file = open("./backend/template/verification.html")
            # subject = file.read().format(code=otp_code, support_mail=email_sender)
            
            em = MIMEText(temp,"html")
            em["From"] = email_sender
            em["To"] = email_reciever
            em["subject"] = mail_title
            

            context = ssl.create_default_context()

            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
                smtp.login(email_sender, email_password)
                smtp.sendmail(email_sender, email_reciever, em.as_string())
            return {"detail":"verification mail sent", "status":"success"}
        except smtplib.SMTPAuthenticationError as e:
            return {"detail":"error sending verification mail", "status":"fail"}

    def token_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            bearer_token = request.headers.get("Authorization")
            
            if not bearer_token:
                return jsonify({"detail": "Token is missing","status":"error"}), 403
            try:          
                data = jwt.decode(bearer_token, secret_key,algorithms=["HS256"])
            except ExpiredSignatureError as e:
                return jsonify({"detail":"Token Expired", "status":"fail"}), 403
            except DecodeError as d:
                return jsonify({"detail":"Incorrect Token", "status":"fail"}), 412
            return f(*args, **kwargs)
        return decorated

    def generate_refresh_token(token):
        decoded = jwt.decode(token,secret_key,["HS256"])
        now = datetime.datetime.time(datetime.datetime.now())
        exp = decoded["exp"]

        difference = exp - now
        check = 0<difference<60.0

        if check is True:
            decoded.pop("exp")
            t = Authentication.generate_access_token(decoded)
            return t
        return ""

    

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
                return {"detail":f" fetching data using primaryKey {primaryKey} not found, provide valid primaryKey", "success":False}
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
        # if tag is none then, by default, it is fetching id from groups
        if tag == "items":
            query = f"""SELECT * FROM {table} WHERE {key}={key_value} """
            cursor.execute(query)
            data = cursor.fetchall()
            if len(data)>0:
                return data, True
            else: 
                return data, False
        
        else:
            query = f"""SELECT {key} FROM {table} """
            cursor.execute(query)
            data = cursor.fetchall()
            xr = list(i[0] for i in cursor.description)
            ls = []
            if len(data)>0:
                for i in data:
                    dic = zip(xr, i)
                    a = dict(dic)
                    ls.append(a)
                    # print(ls)
                return ls, True
            else: 
                return ls, False
            
    def dataFetchUp(self, table, key= "*", key_value=None, tag=None):
            cursor = self.connect()[0].cursor()
            query = f"""SELECT {key} FROM {table} """
            cursor.execute(query)
            data = cursor.fetchall()
            # xr = list(i[0] for i in cursor.description)
            ls = []
            if len(data)>0:
                for i in data:
                    # dic = zip(xr, i)
                    # a = dict(dic)
                    ls.append(i[0])
                    # print(ls)
                return ls, True
            else: 
                return ls, False
    def columnsFetch(self, table, key, key_value=None, tag=None):
        cursor = self.connect()[0].cursor()
        query = f"""SELECT * FROM {table} WHERE {key}={key_value} """
        cursor.execute(query)
        cursor.fetchall()
        ls = list(i[0] for i in cursor.description)
        return ls
    
    
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
    

