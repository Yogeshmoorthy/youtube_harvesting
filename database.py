import pymongo
import pandas as pd
import json


# client = pymongo.MongoClient('mongodb://localhost:27017')
# database = client['Youtube_Scrapping']

def insert_data(collection_name,dataframe,database_name):
    client = pymongo.MongoClient('mongodb://localhost:27017')
    # if database_name != None:
    database = client[database_name]
    collection = database[collection_name]
    data_dict = dataframe.to_dict(orient='records')
    collection.insert_many(data_dict)
    # else:


def fetch_database():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    database_names = client.list_database_names()
    return database_names

