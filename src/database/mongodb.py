import pymongo
import os



import certifi
ca = certifi.where()

class MongodbOperation:
    
    def __init__(self) -> None:
        
        username = os.getenv('MONGO_DB_USERNAME')
        password = os.getenv('MONGO_DB_PASSWORD')
        self.MONGO_DB_URL = f"mongodb+srv://{username}:{password}@cluster0.mboisg7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.client = pymongo.MongoClient(self.MONGO_DB_URL,tlsCAFile=ca)
        self.db_name="kafka_sensor_practice"
        # self.collection_name = "kafka_sensor"

    def insert_many(self,collection_name,records:list):
        self.client[self.db_name][collection_name].insert_many(records)

    def insert(self,collection_name,record):
        self.client[self.db_name][collection_name].insert_one(record)
        
