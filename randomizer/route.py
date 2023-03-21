from flask import Blueprint, request, jsonify
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
    collectionTableName = request.json.get("col_table_name")

    
    if nameOfDb.lower() in support:
        if nameOfDb.lower() == "mongodb":
        #currently, this supports only mongodb
            conn = NoSql(connection_string).connect()
            if conn != None:
                api_key = createKey()
                data = {
                    "_id":api_key,
                    "connect_string":connection_string,
                    "type":db_type, 
                    "dbName":dbName, 
                    "nameOfDb":nameOfDb,
                    "col_table_name":collectionTableName
                }
                try:
                    users.insert_one(data)
                except:
                    api_key = createKey()
                    data["_id"] =  api_key
                    users.insert_one(data)

                return {"success":True, "api_key":api_key, "message":""}, 200
            return {"success":False, "api_key":"", "message":"couldn't connect to NoSQL db using connection string provided"}, 400
        
        else:
            return {"success":False, "api_key":"", "message":f"{nameOfDb} currently not available"}

    return {"success":False, "api_key":"", "message":f"{nameOfDb} not currently supported"}, 400

# don't know what exactly i would be using this for. 
@route.route("/connect", methods=["GET"])
def connect():
    api_key = request.headers.get("api_key")
    check = users.find_one({"_id":api_key})
    if check != None:
        connection_string = check["connect_string"]
        db_name = check["dbName"]
        conn = NoSql(connection_string).db_collection(db_name)
        
        return {"success":True, "message":"connection created successfully"}, 200
    return {"success":False, "message":"Unauthorized access"}, 401

@route.route("/connectionData", methods=["PUT", "GET"])
def updateConnectiondata():
    api_key = request.args.get("api_key")
    user_check = users.find_one({"_id":api_key})
    if user_check!= None:
        if request.method == "GET":
            data = user_check
            data.pop("_id")
            return jsonify({"success":True, "message":"", "data":data}), 200
        if request.method == "PUT":
            connection_string = request.json.get("db_string")
            db_type = request.json.get("db_type")
            nameOfDb = request.json.get("nameOfDB")
            dbName = request.json.get("dbName") #this is the specific name of the db containing the collection that would be used.
            collectionTableName = request.json.get("col_table_name")

            
            if nameOfDb.lower() in support:
                if nameOfDb.lower() == "mongodb":
                #currently, this supports only mongodb
                    conn = NoSql(connection_string).connect()
                    if conn != None:
                        api_key = createKey()
                        data = {
                            "_id":api_key,
                            "connect_string":connection_string,
                            "type":db_type, 
                            "dbName":dbName, 
                            "nameOfDb":nameOfDb,
                            "col_table_name":collectionTableName
                        }
                    return
    return jsonify({"success":False, "message":"Invalid Api-Key"}), 400



