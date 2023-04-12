from flask import Blueprint, request, jsonify
# from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer
from randomizer.function import NoSql, createKey
from randomizer.config import users, db
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
                    db.create_collection(api_key)
                    db_data_list = conn[collectionTableName].find()
                    ls = [str(i["_id"]) for i in db_data_list]
                    db[api_key].insert_one({"items": ls, "tag":"items"})
                    # db[api_key]
                except:
                    api_key = createKey()
                    data["_id"] =  api_key
                    users.insert_one(data)
                    db.create_collection(api_key)
                    db_data_list = conn[collectionTableName].find()
                    ls = [str(i["_id"]) for i in db_data_list]
                    db[api_key].insert_one({"items": ls, "tag":"items"})

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

#  this gets data for specific user by the preference specified.
@route.route("/fetchbyTrofyRating", methods=["GET"])
def fetchBytrofyrating():
    pass

@route.route("/getUserPreference", methods=["POST", "GET", "PUT"])
def userPref():
    api_key = request.headers.get("api_key")
    check = users.find_one({"_id":api_key})
    # print(check)
    if check != None:
        connection_string = check["connect_string"]
        dbName = check["dbName"]
        col_table_name = check["col_table_name"]
        # conn = NoSql(connection_string,dbName).getDatabase()
        if request.method == "GET": # this request gets data based on user's preference.
            user_id = request.args.get("user")
            pref_rating =  request.args.get("pref_rating")
            r = 0.0
            if pref_rating != None:
                r=float(pref_rating)

            if user_id == None:
                return jsonify({"success":False, "message":"'user' querystring cannot be null"}), 400
            user_check = db[api_key].find_one({"_id":user_id, "tag":"user"})
            if user_check != None:
                user_pref_list = list(filter(lambda p: r>p["pref_rating"]< r+5 ,user_check["user_pref"]))
                conn = NoSql(connection_string, dbName).getCollection(col_table_name) #getDatabase(db_name)
                # if type(conn[])==Database:
                #     return {"success":True, "message":"connection created successfully"}, 200
                if conn[1] == True:
                    ls = []
                    for pref_list in user_pref_list:
                        for i in pref_list["item_pref_list"]:
                            cursor = conn[0].find_one({"_id":ObjectId(i)})
                            for item in cursor:
                                keys =[x for x in item.keys()]
                                for key in keys:
                                    if type(item[key]) == ObjectId:
                                        item[key] = str(ObjectId(item[key]))
                                    if type(item[key]) == dict or type(item[key]) == list:
                                        item.pop(key)
                                ls.append(item)
                    # print(ls)
                    return jsonify({"success":True, "message":"", "data":ls}), 200
                else:        
                    return jsonify({"success":False, "message":"unable to connect"}), 400
            return jsonify({"success":False, "message":f" preferences for {user_id} not found"}), 400


        if request.method == "POST": # to get users preference
            info =  request.json
            user_id = info.get("user_id") #user_id
            pref_list = info.get("preference_list") # this indicates a list of moods along with their products list
            # pref_category = info.get("pref_category") # this indicates what product category is being picked.
            if user_id != None:
                if type(pref_list) == dict:
                    pref_keys = [i for i in pref_list.keys()]
                    if "item_pref_list" not in pref_keys:
                        return jsonify({"success":False, "message":" 'item_pref_list' required as a list"}), 400
                    if "pref_rating" not in pref_keys:
                        return jsonify({"success":False, "message":" 'pref_rating' required as string"}), 400
                    up_data = {
                        "_id":user_id,
                        "user_pref":pref_list,
                        "tag":"user"
                    }
                    db[api_key].insert_one(up_data)
                    return jsonify({"success":False, "message":" 'preference_list' type should be a dictionary/object"}), 400

                    

                else:
                    return jsonify({"success":False, "message":" 'preference_list' type should be a dictionary/object"}), 400
            else:return jsonify({"success":False, "message":" 'user_id' cannot be null"}), 400
            



            
        if request.method == "PUT": # to update specific user_preference
            pass

    return jsonify({"success":False, "message":"Invalid Api-Key"}), 400

