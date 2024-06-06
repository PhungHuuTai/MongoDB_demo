import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
# from pre_processing import *

def import_csv_to_mongodb(df, database_name='Ticket_data', collection_name = "Plane", 
                          mongo_uri='mongodb+srv://phunghuutai7:Huutai07admin@democluster1.gf9th9c.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster1'):
    # Read CSV into a Pandas DataFrame

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Convert DataFrame to dictionary for easier MongoDB insertion
    data = df.to_dict(orient='records')

    try:
        # Insert data into MongoDB
        collection.insert_many(data)
        print("Insert data to MongoDB successfully")
    except:
        print("Insert data to MongoDB failed")

    # Close MongoDB connection
    client.close()

if __name__ == "__main__":

    df = pd.read_csv('./ticket_data.csv', delimiter=',')
    import_csv_to_mongodb(df)