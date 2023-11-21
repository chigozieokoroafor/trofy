from pymongo.mongo_client import MongoClient

connect_string = "mongodb+srv://backend:pEL9zYIuV9VeAsWF@oaucv.diq8cit.mongodb.net/?retryWrites=true&w=majority" # this would change later on on to the string they provide. 
# connect_string = "mongodb+srv://chigozie:qwertyuiop@django.95khnms.mongodb.net/test"

connect =  MongoClient(connect_string)

db = connect["TROPY"]

connection_collection = db["conn_col"]
users = db["users"]


connection_collection.create_index("connect_string", unique=True)
users.create_index("email", unique=True)