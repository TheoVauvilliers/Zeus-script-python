from array import array
import pymongo as pm

from pprint import pprint

MONGODB_URI = 'mongodb://admin:admin@localhost:27017/'
MONGODB_DATABASE = 'zeus'

"""
Create the connection to the database
return: {pymongo.database.Database} database
"""
def get_database() -> pm.database.Database:
    client = pm.MongoClient(MONGODB_URI)

    database = client[MONGODB_DATABASE]

    return database

"""
Create the connection to the collection
param: {pymongo.database.Database} database - database connection
param: {string} collection - name of the collection
return: {pymongo.collection.Collection} collection
"""
def get_collection(database: pm.database.Database, collection: str) -> pm.collection.Collection:
    return database[collection]

"""
Insert a row into the collection if it doesn't exist, otherwise update it
param: {pymongo.collection.Collection} collection - collection connection
param: {list} row - row to insert
return: {None}
"""
def insert_row(collection: pm.collection.Collection, row: list) -> None:
    [x, y] = row[3].split(',')

    query = { "user_id": row[1] }
    values = {
        "$push" : {
            "pixels": {
                "timestamp": row[0],
                "pixel_color": row[2],
                "coordinate": {
                    "x": x,
                    "y": y,
                },
            }
        }
    }

    collection.update_one(query, values, upsert=True)
