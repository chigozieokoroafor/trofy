from apscheduler.schedulers.background import BackgroundScheduler
from randomizer.config import users, db
from randomizer.function import NoSql
import asyncio
from bson import ObjectId

# scheduler = BackgroundScheduler(job_defaults={'max_instances': 5000})

users_list = []

async def fetchAPiKeys():
    while True:
        await asyncio.sleep(10)
        try:
            if len(users_list)<1:
                all_users = users.find()
                for i in all_users:
                    users_list.append(i)
                    print(i)
        except UnboundLocalError as e :
            print(e)

async def fetch_user_products(): # not done with this yet
    while True:
        await asyncio.sleep(2)
        if len(users_list) > 0:
            # either create a loop here to check for new items
            # for user_ in user_list:
            try:
                user_ = users_list[0]
                o_col = user_["itemCollection"]
                groupKeyName = user_["groupKeyName"]
                if  user_["type"].lower() == "nosql":
                    collection = NoSql(user_["connect_string"], user_["dbName"]).getCollection(o_col)
                
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
                                        specific_product["trofy_rating"] = rating

                                        product_list.append(specific_product)
                                    db[user_["_id"]].update_one({"_id":i["_id"]}, {"$set":{"products_pref":product_list}})
                                    



                    users_list.pop(0)
            except IndexError as e:
                print("list empty")
    
    
if __name__ =="__main__":
    loop = asyncio.get_event_loop()
    if loop.is_running() == True:
        loop.close()
    asyncio.ensure_future(fetchAPiKeys())
    asyncio.ensure_future(fetch_user_products())
    loop.run_forever()
# continuosly update the items list in the db data.
#  use collection name to get api_keys  and fetch procduct items for the specific category let it be done in the background
