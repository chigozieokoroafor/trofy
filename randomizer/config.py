from pymongo.mongo_client import MongoClient

connect_string = "mongodb+srv://backend:pEL9zYIuV9VeAsWF@oaucv.diq8cit.mongodb.net/?retryWrites=true&w=majority"
connect =  MongoClient(connect_string)

db = connect["TROPY"]

users = db["conn_col"]