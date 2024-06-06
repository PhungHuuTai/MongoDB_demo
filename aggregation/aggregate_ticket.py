import pymongo
import pprint
from pymongo import MongoClient

# Count the number of flights of each type of aircraft
def count_flights(db, cl, mongo_uri):
    client = MongoClient(mongo_uri)
    db = client[db]
    collection = db[cl]

    pipeline = [
        {"$group": {"_id": "$Type", "count": {"$sum": 1}}}
    ]
    aggCursor = collection.aggregate(pipeline)
    
    for doc in aggCursor:
        print(doc)

# Show all flights on '28/09/2023'
def show_flights_on_date(db, cl, mongo_uri):
    client = MongoClient(mongo_uri)
    db = client[db]
    collection = db[cl]

    pipeline = [
        {"$match": {"Date": "2023-09-28"}},
        {"$sort": {"Eco": 1}},
        {"$unset": ["_id", "Date", "Updated"]}
    ]

    results = collection.aggregate(pipeline)
    for doc in results:
        print(doc)

# 

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Ticket_data'
    collection_name = 'Plane'

    # count_flights(db_name, collection_name, mongo_uri)
    show_flights_on_date(db_name, collection_name, mongo_uri)

