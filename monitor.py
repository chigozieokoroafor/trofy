from randomizer.config import users, db
from randomizer.function import Mongodb, SQLType, PostgresqlType
import asyncio
from bson import ObjectId
import threading

# scheduler = BackgroundScheduler(job_defaults={'max_instances': 5000})
# sql_major = ["mysql", "postgresql", "mariadb"]


async def fetchAPiKeys():
    while True:
        await asyncio.sleep(20)
        
        all_users = users.find({})
        for i in all_users:
            if i["nameOfDb"].lower() == "mongodb":
                user = threading.Thread(target=fetch_user_products_mongo, args=(i['_id'], i["itemCollection"], i["groupKeyName"], i["dbName"], i["connect_string"]))
            elif i["nameOfDb"].lower() == "postgresql" or i["nameOfDb"].lower() == "mysql":
                user = threading.Thread(target=fetch_user_products, args=(i['_id'], i["itemTable"], i["groupTable"], i["foreignKey"], i["nameOfDb"], i['connect_string']))
        user.start()

def fetch_user_products(user_id, itemTable, groupTable, foreignKey, nameOfDb, connect_string):
    
    if nameOfDb.lower() == "mysql":
    #    steps
    #    1. connect to db
    #    2. get list of users for specific aorganization
    #    3. access the products table 
    #    4. fetch all items under specific group selected by the user.
    #    5. upload the data for each user on trofy's database. 

    #  1

        # itemTable = user_["itemTable"]
        # foreignKey = user_["foreignKey"]
        try:
            data_fetch = SQLType(connect_string, itemTable).getProductData()
            
            if data_fetch[1]== True:
                # 2
                all_users = db[user_id].find({"tag":"user"}) # this gets users

                for sp_user in all_users:
                    user_pref = sp_user["user_pref"]
                    products_list = []
                    if len(user_pref)>0:
                        for pref in user_pref  :
                            gp_pref_list = pref["item_pref_list"]
                            p_Scale = pref["pref_rating"]
                            
                            # print(data)
                            for product in data_fetch[0]:
                                
                                if product[foreignKey] in gp_pref_list:
                                    product["trofy_rating"] = p_Scale
                                    products_list.append(product)
                            
                                
                        db[user_id].update_one({"_id":sp_user["_id"]}, {"$set":{"products_pref":products_list}})
                    # print(f"done with {sp_user['_id']}")
                    
            else:
                print(f"couldn't collate data for {user_id}")
        except Exception as e:
            print(e)

    elif nameOfDb.lower() == "postgresql":
        try:
            foreignKey = foreignKey.lower() 

                                        
            all_users = db[user_id].find({"tag":"user"}) 
            all_users = [ i for i in all_users]
            
            while len(all_users)>0:
                sp_user = all_users[0]
                
                user_pref = sp_user["user_pref"]
                products_list = []
                if len(user_pref)>0:
                    for pref in user_pref  : # use a while loop here, because it isn't traversing through all the preference objects  for a user.
                        gp_pref_list = pref["item_pref_list"]
                        p_Scale = pref["pref_rating"]
                        
                        for group_id in gp_pref_list: 
                            product_fetch = PostgresqlType(connect_string).dataFetch(itemTable, key=foreignKey, key_value=group_id, tag="items")
                            keys = PostgresqlType(connect_string).columnsFetch(itemTable, key=foreignKey, key_value=group_id, tag="items")
                            if product_fetch[1] == True:
                            
                                product_data = product_fetch[0]
                                for prod in product_data:
                                    a = zip(keys, prod)
                                    json_product = dict(a)
                                    if json_product[foreignKey] in gp_pref_list: # what is meant to happen here is  check if the foreign key is the same as the preference that the user selects.
                                        
                                        json_product["trofy_rating"] = p_Scale
                                        products_list.append(json_product)
                                                    
                                                        
                db[user_id].update_one({"_id":sp_user["_id"]}, {"$set":{"products_pref":products_list}})
                
                
                all_users.pop(0)
        except Exception as e:
             print(e)


def fetch_user_products_mongo(user_id, o_col, groupKeyName, dbName, connect_string):
    collection = Mongodb(connect_string, dbName).getCollection(o_col)            
    comp_users = db[user_id].find({"tag":"user"})
    for i in comp_users:
        users_preferences = i["user_pref"]
        for specific in users_preferences:
            product_list = []
            rating = specific["pref_rating"]
            for group_id in specific["item_pref_list"]:
                group_find = collection[0].find({groupKeyName:ObjectId(group_id)})
                if group_find == None:
                    group_find = collection[0].find({groupKeyName:group_id})
                
                if group_find != None:
                    for specific_product in group_find:
                        product_keys = [key for key in specific_product.keys() if type(key)==ObjectId]
                        for keys in product_keys:
                            specific_product[keys] = str(specific_product[keys])
                        specific_product["trofy_rating"] = float(rating)

                        product_list.append(specific_product)
                    db[user_id].update_one({"_id":i["_id"]}, {"$set":{"products_pref":product_list}})
    
if __name__ =="__main__":
    loop = asyncio.get_event_loop()
    if loop.is_running() == True:
        loop.close()
    asyncio.ensure_future(fetchAPiKeys())
    loop.run_forever()
    
# continuosly update the items list in the db data.
#  use collection name to get api_keys  and fetch procduct items for the specific category let it be done in the background
