from flask import Blueprint, request
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer
from randomizer.function import NoSql, createKey
from randomizer.config import users

support = ["postgresql", "mongodb", "sqlite3"]
auth =  Blueprint("auth", __name__, url_prefix="/api/auth")

@auth.route("/getAPIKey", methods=["POST"])
def getKey():
    connection_string = request.json.get("db_string")
    db_type = request.json.get("db_type")
    db_name = request.json.get("db_name")
    
    if db_name in support:
        #currently, this supports only mongodb
        conn = NoSql(connection_string).connect()
        print(conn)
        if conn != None:
            api_key = createKey()
            data = {
                "_id":api_key,
                "connect_string":connection_string,
                "type":db_type
            }
            try:
                users.insert_one(data)
            except:
                api_key = createKey()
                data["_id"] =  api_key
                users.insert_one(data)

            return {"success":True, "api_key":api_key, "message":""}, 200
        return {"success":False, "api_key":"", "message":"couldn't connect to NoSQL db using connection string provided"}, 400
    return {"success":False, "api_key":"", "message":f"{db_name} not currently supported"}, 400

@auth.route("/connect", methods=["GET"])
def connect():
    api_key = request.headers.get("api_key")
    check = users.find_one({"_id":api_key})
    if check != None:

        return {"success":True, "message":""}, 200
    return {"success":False, "message":"Unauthorized access"}, 401
    




