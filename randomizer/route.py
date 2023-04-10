from flask import Blueprint, request, jsonify
# from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer
from randomizer.function import NoSql, createKey
from randomizer.config import users
import pymongo
from pymongo.database import Database
from bson import ObjectId

support = ["postgresql", "mongodb", "sqlite3", "redis"]

route =  Blueprint("auth", __name__, url_prefix="/api")



@route.route("/getAPIKey", methods=["POST"])
def getKey():
    connection_string = request.json.get("db_string")
    db_type = request.json.get("db_type")
    nameOfDb = request.json.get("nameOfDb")
    dbName = request.json.get("dbName") #this is the specific name of the db containing the collection that would be used.
    collectionTableName = request.json.get("col_table_name")

    
    if nameOfDb.lower() in support:
        if nameOfDb.lower() == "mongodb":
        #currently, this supports only mongodb
            # db = NoSql(connection_string)
            # connection = db.connect()
            conn = NoSql(connection_string,dbName).getDatabase()

            if type(conn) != dict:    
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

 
@route.route("/getDatabaseData", methods=["GET"]) # add sort filters
def fetchD():
    api_key = request.headers.get("api_key")
    filter_key = request.args.get("filter_key")
    limit = request.args.get("limit") or 5
    page = request.args.get("page") or 1
    filter_key_dtype = request.args.get("filter_key_dtype")
    filter_key_value = request.args.get("filter_key_value") #only works for type int, float and double.
    # sorter = request.args.get("sortby")

    filter_data = {}
    if filter_key_dtype == "int":
        filter_data[filter_key] = {"$gte":int(filter_key_value)}
    if filter_key_dtype == "float" or filter_key_dtype == "double":
        filter_data[filter_key] = {"$gte":float(filter_key_value)}
    else:
        filter_data[filter_key] = filter_key_value
    limit =  int(limit)
    page = int(page)
    skip = (page* limit) - limit
    
    # print(filter_data)

    # print(page, limit , skip)

    check = users.find_one({"_id":api_key})
    if check != None:
        connection_string = check["connect_string"]
        db_name = check["dbName"]
        col_table_name = check["col_table_name"]
        try:
            conn = NoSql(connection_string, db_name).getCollection(col_table_name) #getDatabase(db_name)
            # if type(conn[])==Database:
            #     return {"success":True, "message":"connection created successfully"}, 200
            if conn[1] == True:
                cursor = conn[0].find(filter_data).skip(skip).limit(limit)#.sort(sorter)
                ls = []
                for i in cursor:
                    keys =[x for x in i.keys()]
                    for key in keys:
                        if type(i[key]) == ObjectId:
                            i[key] = str(ObjectId(i[key]))
                        if type(i[key]) == dict or type(i[key]) == list:
                            i.pop(key)
                    ls.append(i)
                # print(ls)
                return jsonify({"success":True, "message":"", "data":ls}), 200
            else:        
                return jsonify({"success":False, "message":"unable to connect"}), 400
        except Exception as E:
            return jsonify({"success":False, "message":str(E)}), 400
        
        
    return {"success":False, "message":"Unauthorized access"}, 401


@route.route("/connectionData", methods=["PUT", "GET"])
def updateConnectiondata():
    api_key = request.headers.get("api_key")
    # print(api_key)
    check = users.find_one({"_id":api_key})
    # print(check)
    if check != None:
        if request.method == "GET":
            data = check
            data.pop("_id")
            return jsonify({"success":True, "message":"", "data":data}), 200
        if request.method == "PUT":
            connection_string = request.json.get("db_string")
            db_type = request.json.get("db_type")
            nameOfDb = request.json.get("nameOfDb")
            dbName = request.json.get("dbName") #this is the specific name of the db containing the collection that would be used.
            collectionTableName = request.json.get("col_table_name")

            
            if nameOfDb.lower() in support:
                if nameOfDb.lower() == "mongodb":
                #currently, this supports only mongodb
                    conn = NoSql(connection_string, dbName).getDatabase()
                    if type(conn) != dict:
                        
                        data = {
                            "connect_string":connection_string,
                            "type":db_type, 
                            "dbName":dbName, 
                            "nameOfDb":nameOfDb,
                            "col_table_name":collectionTableName
                        }
                        users.update_one({"_id":api_key}, {"$set":data})
                        return jsonify({"success":True, "message":"connection details uploaded"}), 200
                    return jsonify({"message":"invalid connection string", "success":False}),  400
    return jsonify({"success":False, "message":"Invalid Api-Key"}), 400


@route.route("/fetchbyTrofyRating", methods=["GET"])
def fetchBytrofyrating():
    pass

