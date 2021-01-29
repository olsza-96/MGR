from pymongo import MongoClient
from pymongo.errors import OperationFailure
import logging as log
import time
import pathlib as p
import os
import json
log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")


def connect(host: str, port: int, collection: str):

    start = time.time()
    log.info(f"Connecting to the database")
    connection = MongoClient(host, port)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    with connection:
        log.info(f"Connected to {collection}")
        db = connection.Poland_spatial_data
        current_collection = db[collection]

        cursor = current_collection.aggregate(pipeline=[{"$group": {
                                    "_id": {"id": "$id"},
                                    "uniqueIds": {"$addToSet": "$_id"},
                                    "count": {"$sum": 1}
                                    }},
                                    {"$match": {
                                        "count": {"$gt": 1}
                                        }
                                    },
                                    {"$sort": {
                                        "count": -1
                                        }
                                    }], allowDiskUse = True)

        duplicates = []
        for element in cursor:
            log.info(f"{element}")
            del element["uniqueIds"][0]
            for id in element["uniqueIds"]:
                duplicates.append(id)

        current_collection.remove({"_id": {"$in": duplicates}})

        time.sleep(1)
        end = time.time()
        log.info(f"Process of inserting data took {end - start} seconds")

def delete_elements_col(host: str, port: int, collection: str):
    start = time.time()
    log.info(f"Connecting to the database")
    connection = MongoClient(host, port)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    with connection:
        log.info(f"Connected to {collection}")
        db = connection.Poland_spatial_data
        current_collection = db[collection]

        current_collection.remove({})

        time.sleep(1)
        end = time.time()
        log.info(f"Process of inserting data took {end - start} seconds")


if __name__ == "__main__":
    #connect("localhost", 27017, "nodes")
    delete_elements_col("localhost", 27017, "nodes_final")