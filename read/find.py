from pymongo import MongoClient

def find(db_name, col_name, mongo_uri, key_query, value_query):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[col_name]

    filter_op = {key_query: value_query}
    results = collection.find(filter_op, projection=["Departure", "Arrival", "DepartureTime", "ArrivalTime"])
    return results

def findone(db_name, col_name, mongo_uri, key_query, value_query):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    col = db[col_name]

    filter_op = {key_query: value_query}
    result = col.find_one(filter_op)
    return result

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Ticket_data'
    collection_name = 'Plane'
    result = findone(db_name, collection_name, mongo_uri, "Type", "Airbus A320")
    print(result)
    # result = find(db_name, collection_name, mongo_uri, "No", "VJ144")
    # for f in result:
    #     print(f)
    # Use Find method
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    filter_op = {
        "$and": [
            {"No": "VJ138"},
            {"Eco": {"$lte": 1400000}},
            # {"Business": {"$exists": True}},
            # {"Type": {"$regex": "p{2,}"}}
        ]
    }
    results = collection.find(filter_op, {"_id": 0, "Departure": 1, "Arrival": 1, "DepartureTime": 1, "ArrivalTime": 1})

    for f in results:
        print(f)

    # Use count_documents() method
    filter_op_1 = {
        "$or": [
            {"Eco": {"$gt": 1500000}},
            {"Deluxe": {"$gte": 1800000}}
        ]
    }
    results_1 = collection.count_documents(filter_op_1)
    print(f"\n{results_1}\n")

    # Use distinct() method
    results_2 = collection.distinct("No", {"Type": {"$regex": "321$"}})

    for f in results_2:
        print(f)
    