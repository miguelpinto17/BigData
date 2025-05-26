from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

MONGO_USER = "postgres"
MONGO_PASSWORD = "root"
MONGO_CLUSTER = "cluster0.6p5ublm.mongodb.net"
MONGO_DBNAME = "ObesityDatabase"

uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTER}/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)