import pymongo
from datetime import datetime
from pymongo import MongoClient

def create_data(db, col_1, mongo_uri, col_2):
    client = MongoClient(mongo_uri)
    db = client[db]
    col = db[col_1]

    col.delete_many({})
    order_data = [
        {
            "customer_id": "elise_smith@myemail.com",
            "orderdate": datetime(2020, 5, 30, 8, 35, 52),
            "product_name": "Asus Laptop",
            "product_variation": "Standard Display",
            "value": 431.43
        },
        {
            "customer_id": "tj@wheresmyemail.com",
            "orderdate": datetime(2019, 5, 28, 19, 13, 32),
            "product_name": "The Day Of The Triffids",
            "product_variation": "2nd Edition",
            "value": 5.01
        },
        {
            "customer_id": "oranieri@warmmail.com",
            "orderdate": datetime(2020, 1, 1, 8, 25, 37),
            "product_name": "Morphy Richards Food Mixer",
            "product_variation": "Deluxe",
            "value": 63.13
        },
        {
            "customer_id": "jjones@tepidmail.com",
            "orderdate": datetime(2020, 12, 26, 8, 55, 46),
            "product_name": "Asus Laptop",
            "product_variation": "Standard Display",
            "value": 429.65
        }
    ]
    try:
        # Insert order data into MongoDB
        col.insert_many(order_data)
        print("Insert order data to MongoDB successfully")
    except:
        print("Insert order data to MongoDB failed")

    col_ = db[col_2]
    col_.delete_many({})
    products_data = [
        {
            "name": "Asus Laptop",
            "variation": "Ultra HD",
            "category": "ELECTRONICS",
            "description": "Great for watching movies"
        },
        {
            "name": "Asus Laptop",
            "variation": "Standard Display",
            "category": "ELECTRONICS",
            "description": "Good value laptop for students"
        },
        {
            "name": "The Day Of The Triffids",
            "variation": "1st Edition",
            "category": "BOOKS",
            "description": "Classic post-apocalyptic novel"
        },
        {
            "name": "The Day Of The Triffids",
            "variation": "2nd Edition",
            "category": "BOOKS",
            "description": "Classic post-apocalyptic novel"
        },
        {
            "name": "Morphy Richards Food Mixer",
            "variation": "Deluxe",
            "category": "KITCHENWARE",
            "description": "Luxury mixer turning good cakes into great"
        }
    ]

    try:
        # Insert product data into MongoDB
        col_.insert_many(products_data)
        print("Insert product data to MongoDB successfully")
    except:
        print("Insert product data to MongoDB failed")
    client.close()

def multi_join(db, col, mongo_uri):
    client = MongoClient(mongo_uri)
    db = client[db]
    col = db[col]

    embedded_pl = [
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$eq": ["$product_name", "$$prdname"]},
                        {"$eq": ["$product_variation", "$$prdvartn"]}
                    ]
                }
            }
        },
        {
            "$match": {
                "orderdate": {
                    "$gte": datetime(2020, 1, 1, 0, 0, 0),
                    "$lt": datetime(2021, 1, 1, 0, 0, 0)
                }
            }
        },
        {
            "$unset": ["_id", "product_name", "product_variation"]
        }
    ]
    pipeline = [
        {
            "$lookup": {
                "from": "Orders",
                "let": {
                    "prdname": "$name",
                    "prdvartn": "$variation"
                },
                "pipeline": embedded_pl,
                "as": "Orders"
            }
        },
        {
            "$match": {
                "Orders": {"$ne": []}
            }
        },
        {
            "$unset": ["_id", "description"]
        }
    ]
    results = col.aggregate(pipeline)
    for result in results:
        print(result)

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Order_data'
    collection_name_1 = 'Orders'
    collection_name_2 = "Products"

    # create_data(db_name, collection_name_1, mongo_uri, collection_name_2)
    multi_join(db_name, collection_name_2, mongo_uri)
