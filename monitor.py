from apscheduler.schedulers.background import BackgroundScheduler
from randomizer.config import users, db
from randomizer.function import Mongodb, SQLType, PostgresqlType
import asyncio
from bson import ObjectId

# scheduler = BackgroundScheduler(job_defaults={'max_instances': 5000})
# sql_major = ["mysql", "postgresql", "mariadb"]
users_list = []

async def fetchAPiKeys():
    while True:
        await asyncio.sleep(10)
        
        all_users = users.find()
        for i in all_users:
            # print(users_list)
            if i not in users_list:
                users_list.append(i)

                    
        # except UnboundLocalError as e :
        #     print(e)

async def fetch_user_products(): # not done with this yet this works only for mongodb connections.
    while True:
        await asyncio.sleep(12)
        try:
            if len(users_list) > 0:
                # either create a loop here to check for new items
                # for user_ in user_list:
                try:
                    user_ = users_list.pop(0)
                    # this part for nosql integration.
                    if user_["type"].lower() == "nosql":
                        if user_["nameOfDb"].lower() == "mongodb":
                            o_col = user_["itemCollection"]
                            groupKeyName = user_["groupKeyName"] 
                            collection = Mongodb(user_["connect_string"], user_["dbName"]).getCollection(o_col)
                        
                            comp_users = db[user_["_id"]].find({"tag":"user"})
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
                                            db[user_["_id"]].update_one({"_id":i["_id"]}, {"$set":{"products_pref":product_list}})
    
                    # this part is for sql databases.
                    if user_["type"].lower() == "sql":
                        if user_["nameOfDb"].lower() == "mysql":
                            connect_string = user_["connect_string"]
                        #    steps
                        #    1. connect to db
                        #    2. get list of users for specific aorganization
                        #    3. access the products table 
                        #    4. fetch all items under specific group selected by the user.
                        #    5. upload the data for each user on trofy's database. 

                        #  1

                            itemTable = user_["itemTable"]
                            foreignKey = user_["foreignKey"]
                            
                            data_fetch = SQLType(connect_string, itemTable).getProductData()
                            
                            if data_fetch[1]== True:
                                # 2
                                all_users = db[user_["_id"]].find({"tag":"user"}) # this gets users

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
                                            
                                                
                                        db[user_["_id"]].update_one({"_id":sp_user["_id"]}, {"$set":{"products_perf":products_list}})
                                    # print(f"done with {sp_user['_id']}")
                                    
                            else:
                                print(f"couldn't collate data for {user_['_id']}")
                        
                        elif user_["nameOfDb"].lower() == "postgresql":
                            connect_string = user_["connect_string"]
                                                #    steps
                                                #    1. connect to db
                                                #    2. get list of users for specific aorganization
                                                #    3. access the products table 
                                                #    4. fetch all items under specific group selected by the user.
                                                #    5. upload the data for each user on trofy's database. 

                                                #  1

                            itemTable = user_["itemTable"]
                            foreignKey = user_["foreignKey"].lower() # ensure the strings passed here during creation should be string formatted. 
                                                    
                            # data_fetch = PostgresqlType(connect_string).dataFetch(itemTable, key=foreignKey, tag="item", key_value=) # use tag and keyvalue here

                            # print("############################################")

                                                        # 2
                            all_users = db[user_["_id"]].find({"tag":"user"}) # this gets users
                            all_users = [ i for i in all_users]
                            # print(all_users)
                            for sp_user in all_users:
                                    user_pref = sp_user["user_pref"]
                                    products_list = []
                                    if len(user_pref)>0:
                                        # print(user_pref)
                                        for pref in user_pref  : # use a while loop here, because it isn't traversing through all the preference objects  for a user.
                                            gp_pref_list = pref["item_pref_list"]
                                            p_Scale = pref["pref_rating"]
                                            # print(gp_pref_list)                       
                                            # print("*************************")
                                                                    # print(data)
                                            for group_id in gp_pref_list: 
                                                product_fetch = PostgresqlType(connect_string).dataFetch(itemTable, key=foreignKey, key_value=group_id, tag="items")
                                                keys = PostgresqlType(connect_string).columnsFetch(itemTable, key=foreignKey, key_value=group_id, tag="items")
                                                if product_fetch[1] == True:
                                                # print(product_fetch[0])
                                                    product_data = product_fetch[0]
                                                    for prod in product_data:
                                                        a = zip(keys, prod)
                                                        json_product = dict(a)
                                                        if json_product[foreignKey] in gp_pref_list: # what is meant to happen here is  check if the foreign key is the same as the preference that the user selects.
                                                            
                                                            json_product["trofy_rating"] = p_Scale
                                                            products_list.append(json_product)
                                                                        
                                                                            
                                    db[user_["_id"]].update_one({"_id":sp_user["_id"]}, {"$set":{"products_perf":products_list}})
                                    # print(f"done with {sp_user['_id']}")
                                    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

                        # users_list.pop(0)    
                    try:
                        print(f"done with {user_['_id']}")
                    except KeyError as e:
                        pass
                    # users_list.pop(0)
                except IndexError as e:
                    print("list empty")
        except Exception as e:
            print(e)
# create   async function to pwriodically update item groups in db.
# for sql figure out how it would be different. 

if __name__ =="__main__":
    loop = asyncio.get_event_loop()
    if loop.is_running() == True:
        loop.close()
    asyncio.ensure_future(fetchAPiKeys())
    asyncio.ensure_future(fetch_user_products())
    loop.run_forever()
# continuosly update the items list in the db data.
#  use collection name to get api_keys  and fetch procduct items for the specific category let it be done in the background
