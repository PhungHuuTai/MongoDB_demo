import pymongo
from datetime import datetime
from pymongo import MongoClient

def create_data(db, col, mongo_uri, new_col):
    client = MongoClient(mongo_uri)
    db = client[db]
    col = db[col]

    col.delete_many({})
    order_data = [
        {
            "customer_id": "elise_smith@myemail.com",
            "orderdate": datetime(2020, 5, 30, 8, 35, 52),
            "product_id": "a1b2c3d4",
            "value": 431.43
        },
        {
            "customer_id": "tj@wheresmyemail.com",
            "orderdate": datetime(2019, 5, 28, 19, 13, 32),
            "product_id": "z9y8x7w6",
            "value": 5.01
        },
        {
            "customer_id": "oranieri@warmmail.com",
            "orderdate": datetime(2020, 1, 1, 8, 25, 37),
            "product_id": "ff11gg22hh33",
            "value": 63.13
        },
        {
            "customer_id": "jjones@tepidmail.com",
            "orderdate": datetime(2020, 12, 26, 8, 55, 46),
            "product_id": "a1b2c3d4",
            "value": 429.65
        }
    ]
    try:
        # Insert order data into MongoDB
        col.insert_many(order_data)
        print("Insert order data to MongoDB successfully")
    except:
        print("Insert order data to MongoDB failed")

    col_ = db[new_col]
    product_data = [
        {
            "id": "a1b2c3d4",
            "name": "Asus Laptop",
            "category": "ELECTRONICS",
            "description": "Good value laptop for students"
        },
        {
            "id": "z9y8x7w6",
            "name": "The Day Of The Triffids",
            "category": "BOOKS",
            "description": "Classic post-apocalyptic novel"
        },
        {
            "id": "ff11gg22hh33",
            "name": "Morphy Richardds Food Mixer",
            "category": "KITCHENWARE",
            "description": "Luxury mixer turning good cakes into great"
        },
        {
            "id": "pqr678st",
            "name": "Karcher Hose Set",
            "category": "GARDEN",
            "description": "Hose + nosels + winder for tidy storage"
        }
    ]
    try:
        # Insert product data into MongoDB
        col_.insert_many(product_data)
        print("Insert product data to MongoDB successfully")
    except:
        print("Insert product data to MongoDB failed")
    client.close()

def one_to_one_join(db, col, mongo_uri):
    client = MongoClient(mongo_uri)
    db = client[db]
    col = db[col]

    pipeline = [
        {
            "$match": {
                "orderdate": {
                    "$gte": datetime(2020, 1, 1, 0, 0, 0),
                    "$lt": datetime(2021, 1, 1, 0, 0, 0)
                }
            }
        },
        {
            "$lookup": {
                "from": "Products",
                "localField": "product_id",
                "foreignField": "id",
                "as": "product_mapping"
            }
        },
        {
            "$set": {
                "product_mapping": {"$first": "$product_mapping"}
            }
        },
        {
            "$set": {
                "product_name": "$product_mapping.name",
                "product_category": "$product_mapping.category"
            }
        },
        {
            "$unset": ["_id", "product_id", "product_mapping"]
        }
    ]
    results = col.aggregate(pipeline)
    for result in results:
        print(result)

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Order_data'
    collection_name = 'Orders'
    new_collection = "Products"

    # create_data(db_name, collection_name, mongo_uri, new_collection)
    one_to_one_join(db_name, collection_name, mongo_uri)
