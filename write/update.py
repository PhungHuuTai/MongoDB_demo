from pymongo import MongoClient
import pandas as pd

def update_mongodb(database_name, collection_name, mongo_uri, key_query, value_query, key_update, value_update):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    query_filter = {key_query: value_query}
    update_operator = { '$set': {key_update: value_update}}
    result = collection.update_many(query_filter, update_operator, upsert=True)
    return result

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Ticket_data'
    collection_name = 'Plane'

    result = update_mongodb(db_name, collection_name, mongo_uri, 'No', 'VJ126', 'Departure', 'HCM')
    print(result)
    