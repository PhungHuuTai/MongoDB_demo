from pymongo import MongoClient
import pandas as pd

def update_mongodb(database_name, collection_name, mongo_uri, key_query, value_query):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    query_filter = {key_query: value_query}
    result = collection.delete_many(query_filter)
    return result

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Ticket_data'
    collection_name = 'Plane'

    result = update_mongodb(db_name, collection_name, mongo_uri, 'No', 'VJ162')
    print(result)
    