from pymongo import MongoClient
import pymongo
import pandas as pd

def update_mongodb(database_name, collection_name, mongo_uri, key_query, value_query, key_update, value_update):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    operators = [
        pymongo.InsertOne(
            {
                "Date": "29/09/2023",
                "No": 'VJ311',
                "Type": "Airbus A300",
                "Departure": "HUI",
                "Arrival": "HCM",
                "DepartureTime": "21:35",
                "ArrivalTime": "23:05",
                "Eco": 1240000
            }
        ),
        pymongo.InsertOne(
            {
                "Date": "29/09/2023",
                "No": 'VJ311',
                "Type": "Airbus A300",
                "Departure": "HCM",
                "Arrival": "HUI",
                "DepartureTime": "18:05",
                "ArrivalTime": "19:35",
                "Deluxe": 1350000,
                "Eco": 1190000
            }
        ),
        pymongo.UpdateMany(
            {key_query: value_query},
            {'$set': {key_update: value_update}}
        ),
        pymongo.DeleteOne(
            {key_query: value_query}
        )
    ]

    result = collection.bulk_write(operators)
    return result

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Ticket_data'
    collection_name = 'Plane'

    result = update_mongodb(db_name, collection_name, mongo_uri, 'No', 'VJ311', 'Eco', 1250000)
    print(result)
    