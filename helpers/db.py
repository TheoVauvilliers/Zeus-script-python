import pymongo as pm

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
Drop the collection if it exists and create a new one with an index
param: {pymongo.database.Database} database - database connection
param: {string} collection_name - name of the collection
return: {pymongo.collection.Collection} collection
"""
def init_and_get_collection(database: pm.database.Database, collection_name: str) -> pm.collection.Collection:
    collection = database[collection_name]
    collection.drop()
    collection.create_index([ ("user_id", 1) ], unique=True)
    return collection   

"""
Execute the bulk operation
param: {pymongo.collection.Collection} collection - collection connection
param: {list} rows - list of rows to insert
return: None
"""
def bulk_execute(collection: pm.collection.Collection, rows: list) -> None:
    data = []

    for row in rows:
        x, y, *_ = row[3].split(',')
        user = { "user_id": row[1] }
        value = {
            "$inc": { "number_of_pixels": 1 },
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

        data.append(pm.UpdateOne(user, value, upsert=True))
    
    collection.bulk_write(data, ordered=False)
