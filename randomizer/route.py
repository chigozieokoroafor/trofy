from flask import Blueprint, request, jsonify
# from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer
from randomizer.function import Mongodb, createKey, SQLType, PostgresqlType
from randomizer.config import users, db
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
import collections
import random



# from bson.errors import 

# support = ["postgresql", "mongodb", "sqlite3", "redis"]
support = {
    'nosql':["mongodb", "redis"],
    "sql":["postgresql", "mariadb", "mysql"]
}

route =  Blueprint("auth", __name__, url_prefix="/api")

@route.route("/healthCheck", methods=["GET"])
def h():
    return jsonify({"detail":"Health check success"}), 200

@route.route("/getAPIKey", methods=["POST"])
def getKey():
    #  this part is for mongodb nosql database
    db_type = request.json.get("db_type")
    
    if db_type.lower() == "nosql":
        connection_string = request.json.get("db_string")    
        nameOfDb = request.json.get("nameOfDb")
        dbName = request.json.get("dbName")
         #this is the specific name of the db containing the collection that would be used.
        collectionTableName = request.json.get("groupCollection") # this specifies the collection or table containing data using in grouping products
        itemCollection = request.json.get("itemCollection")
        groupKeyName = request.json.get("groupKeyName") # this is the key used to identify the products in a specific group.

    
        if nameOfDb.lower() in support[db_type.lower()]:
            if nameOfDb.lower() == "mongodb":
            #currently, this supports only mongodb
                # db = Mongodb(connection_string)
                # connection = db.connect()
                conn = Mongodb(connection_string,dbName).getDatabase()

                if type(conn) != dict:    
                    api_key = createKey()
                    data = {
                        "_id":api_key,
                        "connect_string":connection_string,
                        "type":db_type, 
                        "dbName":dbName, 
                        "nameOfDb":nameOfDb,
                        "groupCollection":collectionTableName,
                        "itemCollection":itemCollection,
                        "groupKeyName":groupKeyName
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
                return {"success":False, "api_key":"", "message":"couldn't connect to Mongodb db using connection string provided"}, 400

            else:
                return {"success":False, "api_key":"", "message":f"{nameOfDb} currently not supported"}
            
    if db_type.lower()  == "sql":
        url = request.json.get("db_string")
        groupTable = request.json.get("groupTable") # this specifies the collection or table containing data using in grouping products
        itemTable = request.json.get("itemTable")
        primaryKey = request.json.get("primaryKey") # this is the name of the primaryKey that is used to store the group_id
        foreignKey = request.json.get("foreignKey") # this is the key that links products to specific groups.
        nameOfDb = request.json.get("nameOfDb")

        if nameOfDb.lower() in support[db_type.lower()]:
            if nameOfDb.lower() == "mysql" :
                # connection_check = SQLType(url, groupTable).getTable(primaryKey)
                connection_check = SQLType(url, groupTable).connect()
                
                if connection_check[1] == False:
                    return jsonify({"success":False, "api_key":"", "message":connection_check[0]}), 400
                else:
                    groupFetch = SQLType(url, groupTable).getTableData(primaryKey)
                    if groupFetch[1] == False:
                        return  jsonify({"success":False, "api_key":"", "message":groupFetch["detail"]}), 400
                    
                    # not sure if this product check is feasible.
                    # productFetch = SQLType(url, groupTable).getTableData(foreignKey)
                    # if type(productFetch) != bool:
                    #     return  jsonify({"success":False, "api_key":"", "message":productFetch["detail"]}), 400
                    else:
                        api_key = createKey()
                        data = {
                            "_id":api_key,
                            "connect_string":url,
                            "type":db_type, 
                            "nameOfDb":nameOfDb,
                            "groupTable":groupTable,
                            "itemTable":itemTable,
                            "primaryKey":primaryKey,
                            "foreignKey":foreignKey
                        }

                        try:
                            users.insert_one(data)
                            db.create_collection(api_key)
                            db_data_list = SQLType(url, groupTable).getTableData(primaryKey) # will have to test this out
                            
                            if db_data_list[1] == True:
                                ls = db_data_list[0]
                                db[api_key].insert_one({"items": ls, "tag":"items"})
                            else:
                                return {"success":False, "api_key":"", "message":f"{nameOfDb} not currently supported"}, 400 
                        except:
                            api_key = createKey()
                            data["_id"] =  api_key
                            users.insert_one(data)
                            db.create_collection(api_key)
                            db_data_list = SQLType(url, groupTable).getTableData(primaryKey) # will have to test this out
                            
                            if db_data_list[1] == True:
                                ls = db_data_list[0]
                                db[api_key].insert_one({"items": ls, "tag":"items"})
                            else:
                                return {"success":False, "api_key":"", "message":f"{nameOfDb} not currently supported"}, 400 
                        return {"success":True, "api_key":api_key, "message":""}, 200

            if  nameOfDb.lower() == "postgresql":
                connection_check = PostgresqlType(url).connect()
                
                if connection_check[1] == False:
                    return jsonify({"success":False, "api_key":"", "message":connection_check[0]}), 400
                else:
                    # this part checks for the group table
                    groupFetch = PostgresqlType(url).tableCheck(groupTable)
                    if groupFetch == False:
                        return  jsonify({"success":False, "api_key":"", "message":f"'{groupTable}' doesn't exist"}), 400 
                    # this part checks for the product table
                    pFetch = PostgresqlType(url).tableCheck(itemTable)
                    if pFetch == False:
                        return  jsonify({"success":False, "api_key":"", "message":f"'{itemTable}' doesn't exist"}), 400
                    
                    api_key = createKey()
                    data = {
                        "_id":api_key,
                        "connect_string":url,
                        "type":db_type, 
                        "nameOfDb":nameOfDb,
                        "groupTable":groupTable,
                        "itemTable":itemTable,
                        "primaryKey":primaryKey,
                        "foreignKey":foreignKey
                    }

                    try:
                        users.insert_one(data)
                    except:
                        api_key = createKey()
                        data["_id"] =  api_key
                        users.insert_one(data)

                    
                    db.create_collection(api_key)
                    db_data_list = PostgresqlType(url).dataFetch(itemTable, primaryKey) # will have to test this out
                    
                    
                    ls = db_data_list[0]
                    db[api_key].insert_one({"items": ls, "tag":"items"})

                    return {"success":True, "api_key":api_key, "message":""}, 200


    
    return {"success":False, "api_key":"", "message":f"{nameOfDb} not currently supported"}, 400

 
@route.route("/fetchGroups", methods=["GET"]) # add sort filters
def fetchD():
    api_key = request.headers.get("api_key")
    filter_key = request.args.get("filter_key")
    limit = request.args.get("limit") or 5
    page = request.args.get("page") or 1
    filter_key_dtype = request.args.get("filter_key_dtype")
    filter_key_value = request.args.get("filter_key_value") #only works for type int, float and double.
    # sorter = request.args.get("sortby")

    
    
    # print(filter_data)

    # print(page, limit , skip)

    check = users.find_one({"_id":api_key})
    if check != None:
        connection_string = check["connect_string"]

        if check["nameOfDb"].lower() == "mongodb":
            
            # this filter block would be moved to accomodate mysql and postgresql DBs later on !!!!!
            filter_data = {}
            if filter_key != None:
                if filter_key_dtype == "int":
                    filter_data[filter_key] = {"$gte":int(filter_key_value)}
                if filter_key_dtype == "float" or filter_key_dtype == "double":
                    filter_data[filter_key] = {"$gte":float(filter_key_value)}
                else:
                    filter_data[filter_key] = filter_key_value
            limit =  int(limit)
            page = int(page)
            skip = (page * limit) - limit


            db_name = check["dbName"]
            col_table_name = check["groupCollection"]
            try:
                conn = Mongodb(connection_string, db_name).getCollection(col_table_name) #getDatabase(db_name)
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
        
        
        if check["nameOfDb"].lower() == "mysql":
            dbName = check["dbName"]
            groupTable = check["groupTable"]
            data = SQLType(connection_string, groupTable).getGroupData()

            # add the filters later on
            
            if data[1] == True:
                return jsonify({"success":True, "message":"", "data":data[0]}), 200
        
        if check["nameOfDb"].lower() == "postgresql":
            groupTable = check["groupTable"]
            data = PostgresqlType(connection_string).dataFetch(groupTable)
            return jsonify({"success":True, "message":"", "data":data[0]}), 200
        
            # data = SQLType(connection_string, groupTable).getGroupData()


        
    return {"success":False, "message":"Unauthorized access"}, 401

# reqrite the put request to be able to update the new changes in the getAPIKey request.
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
            db_type = request.json.get("db_type")

            if db_type.lower() == "nosql":
                connection_string = request.json.get("db_string")
                
                nameOfDb = request.json.get("nameOfDb")
                dbName = request.json.get("dbName") #this is the specific name of the db containing the collection that would be used.
                groupCollection = request.json.get("groupCollection")
                itemCollection = request.json.get("itemCollection")

                if nameOfDb.lower() == "mongodb":
                #currently, this supports only mongodb
                    conn = Mongodb(connection_string, dbName).getDatabase()
                    if type(conn) != dict:
                        
                        data = {
                            "connect_string":connection_string,
                            "type":db_type, 
                            "dbName":dbName, 
                            "nameOfDb":nameOfDb,
                            "groupCollection":groupCollection,
                            "itemCollection":itemCollection
                        }
                        users.update_one({"_id":api_key}, {"$set":data})
                        return jsonify({"success":True, "message":"connection details uploaded"}), 200
                    return jsonify({"message":"invalid connection string", "success":False}),  400

            if db_type.lower() == "sql":
                url = request.json.get("db_string")
                groupTable = request.json.get("groupTable") # this specifies the collection or table containing data using in grouping products
                itemTable = request.json.get("itemTable")
                primaryKey = request.json.get("primaryKey") # this is the name of the primaryKey that is used to store the group_id
                foreignKey = request.json.get("foreignKey") # this is the key that links products to specific groups.
                nameOfDb = request.json.get("nameOfDb")

                if nameOfDb.lower == "sql":
                    pass
                if nameOfDb.lower == "postgresql":
                    bd_string_check = PostgresqlType(url)
                    if bd_string_check[1] == True:
                        
                        for t in [groupTable, itemTable]:
                            table_check = PostgresqlType(url).tableCheck(t)
                            if table_check == False:
                                return jsonify({"success":False, "message":f"{t} table doesn't exist"}), 400

                        data = {
                            "connect_string":url,
                                "type":db_type, 
                                "dbName":dbName, 
                                "nameOfDb":nameOfDb,
                                "groupTable":groupTable,
                                "itemTable":itemTable,
                                "primaryKey":primaryKey,
                                "foreignKey":foreignKey
                            }
                        
                        users.update_one({"_id":api_key}, {"$set":data})

                        return jsonify({"success":True, "message":"connection details uploaded"}), 200
                        
                    else:
                        return jsonify({"message":"invalid connection string", "success":False}),  400



                

    return jsonify({"success":False, "message":"Invalid Api-Key"}), 400


@route.route("/getUserPreference", methods=["POST", "GET", "PUT"])
def userPref():
    api_key = request.headers.get("api_key")
    if api_key == None:
        return jsonify({"success":False, "message":"api_key required"}), 400
    check = users.find_one({"_id":api_key})
    if check != None:
        # connection_string = check["connect_string"]
        # dbName = check["dbName"]
        
        # conn = Mongodb(connection_string,dbName).getDatabase()
        if request.method == "GET": # this request gets data based on user's preference.
            user_id = request.args.get("user")
            pref_rating =  request.args.get("pref_rating")
            # itemCollection = check["itemCollection"] 
            r = 5.0
            if pref_rating != None:
                r=float(pref_rating)

            if user_id == None:
                return jsonify({"success":False, "message":"'user' querystring cannot be null"}), 400
            user_check = db[api_key].find_one({"_id":user_id, "tag":"user"})
            
            if user_check != None:
                # user_pref_list = list(filter(lambda p: r>p["pref_rating"]< r+5 ,user_check["user_pref"]))
                # print(user_pref_list)
                # print(user_check["products_pref"])
                # print(r)
                # user_pref_list = list(filter(lambda p: r>p["trofy_rating"]< r+5, user_check["products_pref"]))
                # print(user_pref_list)
                user_pref_list = []
                for i in user_check["products_perf"]:
                    if r < i["trofy_rating"] < r+3.0:
                                keys =[x for x in i.keys()]
                                for key in keys:
                                    if type(i[key]) == ObjectId:
                                        i[key] = str(ObjectId(i[key]))
                                    if type(i[key]) == dict or type(i[key]) == list:
                                        i.pop(key)
                                user_pref_list.append(i)
                rand = []
                if len(user_pref_list) != 0:
                    rand =  random.choices(user_pref_list, k=3)   
                # conn = Mongodb(connection_string, dbName).getCollection(itemCollection) #getDatabase(db_name)
                # if type(conn[])==Database:
                #     return {"success":True, "message":"connection created successfully"}, 200
                # if conn[1] == True:
                #     ls = []
                #     for pref_list in user_pref_list:
                #         for i in pref_list["item_pref_list"]:
                #             cursor = conn[0].find_one({"_id":ObjectId(i)})
                #             for item in cursor:
                #                 keys =[x for x in item.keys()]
                #                 for key in keys:
                #                     if type(item[key]) == ObjectId:
                #                         item[key] = str(ObjectId(item[key]))
                #                     if type(item[key]) == dict or type(item[key]) == list:
                #                         item.pop(key)
                                # ls.append(item)
                    # print(ls)
                return jsonify({"success":True, "message":"", "data":rand}), 200
                
            return jsonify({"success":False, "message":f" preferences for {user_id} not found"}), 400


        if request.method == "POST": # to add new preferences
            info =  request.json
            user_id = info.get("user_id") #user_id
            pref_list = info.get("preference_list") # this indicates a list of moods along with their products list
            # pref_category = info.get("pref_category") # this indicates what product category is being picked.
            if user_id != None:
                if type(pref_list) == list:
                    if len(pref_list) > 0:
                        for single_items in pref_list:
                            pref_keys = [i for i in single_items.keys()]

                            if "item_pref_list" not in pref_keys:
                                return jsonify({"success":False, "message":" 'item_pref_list' required as a list"}), 400
                            if "pref_rating" not in pref_keys:
                                return jsonify({"success":False, "message":" 'pref_rating' required as string"}), 400
                        up_data = {
                        "_id":user_id,
                        "user_pref":pref_list,
                        "tag":"user"
                        }
                        try:
                            db[api_key].insert_one(up_data)
                            return jsonify({"success":True, "message":"Preferences uploaded.", "data":[]}), 200
                        except DuplicateKeyError as e:
                            db[api_key].update_one({"_id":user_id},{"$set": {"user_pref":pref_list}})
                            return jsonify({"success":True, "message":"Preferences updated.", "data":[]}), 200
                        
                    # return jsonify({"success":False, "message":" 'preference_list' type should be a list of dictionaries/object"}), 400
                    
                    

                else:
                    return jsonify({"success":False, "message":" 'preference_list' type should be a list of dictionaries/object"}), 400
            else:return jsonify({"success":False, "message":" 'user_id' cannot be null"}), 400
            
            
        

    return jsonify({"success":False, "message":"Invalid Api-Key"}), 400

