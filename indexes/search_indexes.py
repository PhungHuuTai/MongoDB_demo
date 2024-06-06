import pymongo
from pymongo import MongoClient

def create_search_indexes(db_name, col_name, mongo_uri, field_name_1, field_name_2):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[col_name]
    
    
def list_search_indexes(db_name, col_name, mongo_uri, field_name_1, field_name_2):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[col_name]
    index_1 = {
        "definition": {
            "mappings": {
                "dynamic": True
            }
        },
        field_name_1: "first_index",
    }
    index_2 = {
        "definition": {
            "mappings": {
                "dynamic": True
            }
        },
        field_name_2: "second_index",
    }
    indexes = [index_1, index_2]

    collection.create_search_indexes(models=indexes)
    results = list(collection.list_search_indexes())
    for index in results:
        print(index)

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Ticket_data'
    collection_name = 'Plane'
    
    list_search_indexes(db_name, collection_name, mongo_uri, "No", "Type")
    
