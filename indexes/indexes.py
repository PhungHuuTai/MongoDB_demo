import pymongo
from pymongo import MongoClient

def create_single_field(db_name, col_name, mongo_uri, field_name, value):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[col_name]

    collection.create_index(field_name)
    query = {field_name: value}
    sort = [(field_name, 1)]
    
    result = collection.find(query, {"_id": 0}, sort=sort)
    return result

def create_compound_field(db_name, col_name, mongo_uri, field_name_1, field_name_2, value_1, value_2):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[col_name]

    collection.create_index([(field_name_1, pymongo.ASCENDING), (field_name_2, pymongo.ASCENDING)])
    query = {
        field_name_1: value_1,
        field_name_2: {"$gt": value_2}
    }
    sort = [(field_name_1, pymongo.ASCENDING), (field_name_2, pymongo.ASCENDING)]
    
    result = collection.find(query, {"_id": 0}, sort=sort)
    return result

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Ticket_data'
    collection_name = 'Plane'

    # result = create_single_field(db_name, collection_name, mongo_uri, "No", "VJ198")
    # for f in result:
    #     print(f)
    
    result_1 = create_compound_field(db_name, collection_name, mongo_uri, "Type", "Deluxe", "Airbus A320", 1800000)
    for f in result_1:
        print(f)
