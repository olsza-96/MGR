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
                                    "_id": {"name": "$name"},
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


def get_nodes_from_way(host: str, port: int):
    log.info(f"Connecting to the database")
    connection = MongoClient(host, port)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    db = connection.Poland_spatial_data
    attributes = {"nodes": 1, "landuse": 1, "id":1, "_id": 0}
    for i in range(320, 330):
        start = time.time()
        log.info(f"Getting nodes for region {i}")
        update_nodes_with_landuse(i, attributes, db)
        time.sleep(1)
        end = time.time()
        log.info(f"Process took {(end - start) / 60} minutes")



def update_nodes_with_landuse(region_id: int,  attributes: dict, db) -> None:
    log.info("Getting allowable nodes data from collection")
    cursor = db["ways"].find({"region_id": region_id}, attributes, allow_disk_use = True)
    data = list(cursor)
    if len(data) != 0:
        for element in data:
            log.info(f"Way_id: {element['id']}")
            db["testing_col"].update_many({"id": {"$in": element["nodes"]}},
                                     {"$set": {"landuse": element["landuse"],
                                               "way_id": element["id"]}},
                                     upsert=False
                                      )



def insert_to_collection(document: dict, collection):

    #cursor_check = collection.find({"id": document["id"]})
    #if cursor_check.count() == 0:
    #    log.info("Cursor is empty")
    #    collection.insert_one(document)
    #else:
    #    log.info(f"The cursor for {document['id']} already exists")

    collection.insert_one(document)




if __name__ == "__main__":
    get_nodes_from_way("localhost", 27017)
    #connect("localhost", 27017, "regions")
    #delete_elements_col("localhost", 27017, "regions")