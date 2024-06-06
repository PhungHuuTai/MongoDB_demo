import pymongo
import datetime
from pymongo import MongoClient

# Count the number of comment of each user
def count_comments(db, col, uri):
    client = MongoClient(uri)
    db = client[db]
    col = db[col]

    pipeline = [
        {"$match": 
            {"date": {"$gte": datetime.datetime(2010, 1, 1, 0, 0, 0)}}
        },
        {"$sort": {"date": 1}},
        {
            "$group": {
                "_id": "$name",
                "total_comment": {"$sum": 1},
            }
        },
        {"$set": {"customer_id": "$_id"}},
        {"$unset": ["_id"]}
    ]
    results = col.aggregate(pipeline)
    for result in results:
        print(result)

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'sample_mflix'
    collection_name = 'comments'

    count_comments(db_name, collection_name, mongo_uri)
