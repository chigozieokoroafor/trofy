from flask import Blueprint, request
# from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer
from randomizer.function import NoSql, createKey
from randomizer.config import users

support = ["postgresql", "mongodb", "sqlite3", "redis"]
route =  Blueprint("auth", __name__, url_prefix="/api")

@route.route("/getAPIKey", methods=["POST"])
def getKey():
    connection_string = request.json.get("db_string")
    db_type = request.json.get("db_type")
    nameOfDb = request.json.get("nameOfDB")
    dbName = request.json.get("dbName") #this is the specific name of the db containing the collection that would be used.

    
    if nameOfDb in support:
        #currently, this supports only mongodb
        conn = NoSql(connection_string).connect()
        if conn != None:
            api_key = createKey()
            data = {
                "_id":api_key,
                "connect_string":connection_string,
                "type":db_type, 
                "dbName":dbName, 
                "nameOfDb":nameOfDb
            }
            try:
                users.insert_one(data)
            except:
                api_key = createKey()
                data["_id"] =  api_key
                users.insert_one(data)

            return {"success":True, "api_key":api_key, "message":""}, 200
        return {"success":False, "api_key":"", "message":"couldn't connect to NoSQL db using connection string provided"}, 400
    return {"success":False, "api_key":"", "message":f"{nameOfDb} not currently supported"}, 400

# don't know what exactly i would be using this for. 
@route.route("/connect", methods=["GET"])
def connect():
    api_key = request.headers.get("api_key")
    check = users.find_one({"_id":api_key})
    if check != None:
        return {"success":True, "message":"connection created successfully"}, 200
    return {"success":False, "message":"Unauthorized access"}, 401
    




