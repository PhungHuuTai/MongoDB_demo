import pymongo
from pymongo import MongoClient

def create_db(db, col, mongo_uri):
    client = MongoClient(mongo_uri)
    db = client[db]
    col = db[col]

    order_data = [
        {
            "order_id": 6363763262239,
            "products": [
                {
                    "prod_id": "abc12345",
                    "name": "Asus Laptop",
                    "price": 431,
                },
                {
                    "prod_id": "def45678",
                    "name": "Karcher Hose Set",
                    "price": 22,
                },
            ]
        },
        {
            "order_id": 1197372932325,
            "products": [
                {
                    "prod_id": "abc12345",
                    "name": "Asus Laptop",
                    "price": 429,
                }
            ]
        },
        {
            "order_id": 9812343774839,
            "products": [
                {
                    "prod_id": "pqr88223",
                    "name": "Morphy Richards Food Mixer",
                    "price": 431,
                },
                {
                    "prod_id": "def45678",
                    "name": "Karcher Hose Set",
                    "price": 21,
                }
            ]
        },
        {
            "order_id": 4433997244387,
            "products": [
                {
                    "prod_id": "def45678",
                    "name": "Karcher Hose Set",
                    "price": 23,
                },
                {
                    "prod_id": "jkl77336",
                    "name": "Picky Pencil Sharpener",
                    "price": 1,
                },
                {
                    "prod_id": "xyz11228",
                    "name": "Russell Hobbs Chrome Kettle",
                    "price": 16,
                }
            ]
        }
    ]

    try:
        # Insert data into MongoDB
        col.insert_many(order_data)
        print("Insert data to MongoDB successfully")
    except:
        print("Insert data to MongoDB failed")

    client.close()

def unpack_array(db, col, mongo_uri):
    client = MongoClient(mongo_uri)
    db = client[db]
    col = db[col]

    pipeline = [
        {"$unwind": {"path": "$products"}},
        {"$match": {"products.price": {"$gt": 15}}},
        {
            "$group": {
                "_id": "$products.prod_id",
                "product": {"$first": "$products.name"},
                "total_value": {"$sum": "$products.price"},
                "quantity": {"$sum": 1}
            }
        },
        {"$set": {"product_id": "$_id"}},
        {"$unset": ["_id"]}
    ]

    results = col.aggregate(pipeline)
    for result in results:
        print(result)

if __name__ == "__main__":
    mongo_uri = 'mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'
    db_name = 'Order_data'
    collection_name = 'Orders'
        
    create_db(db_name, collection_name, mongo_uri)
    unpack_array(db_name, collection_name, mongo_uri)
