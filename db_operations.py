from pymongo import MongoClient, DeleteMany
from pymongo.errors import OperationFailure, BulkWriteError
import logging as log
import time
import pathlib as p
import os
import json
import ssl
from collections import defaultdict

log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")


from bson import json_util, ObjectId, BSON, objectid
import json

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)

def return_duplicate_list():

    start = time.time()
    log.info(f"Connecting to the database")

    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    db = connection.Poland_spatial_data
    collection = db["testing_col"]
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    with connection:
        log.info(f"Connected to collection")
        cur = collection.find({}, {"id": 1, "_id": 1})
        data = list(cur)

        items = defaultdict(list)
        for row in data:
            items[row['id']].append(row['_id'])  #make a list of 'id' values for each 'id' key

        ids_to_drop = []
        log.info("Creating list of duplicate items")
        for key in items.keys():
            if len(items[key]) > 1:  #if there is more than one 'id'
                ids_to_drop.append(items[key][1])  #drop the second occurence of id


        #cur = collection.find({"region_id": 1}, {"_id": 1})
        #data = list(cur)
        #data_to_save = [x["_id"] for x in ids_to_drop]
        test = json_util.dumps(ids_to_drop)

        #Dump loaded BSON to valid JSON string and reload it as dict
        with open("duplicates.json", "w") as write_file:
            json.dump(test, write_file)


        """data = list(cur)

        json.encode(cur, cls=JSONEncoder)

        with open("duplicates.json", "w") as write_file:
            json.dump(cur, write_file)"""

def remove_duplicates():
    start = time.time()

    with open("duplicates.txt", "r") as read_file:
        file = read_file.readlines()
    file = file[0].replace("[", "").replace("]", "")
    file_list = file.split(",")
    file_list = [x.strip(" ").strip('"') for x in file_list]

    #encoded = BSON.encode(file)
    list_ids = [objectid.ObjectId(x) for x in file_list]
    log.info(list_ids)
    a = 1
    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    db = connection.Poland_spatial_data
    collection = db["testing_col"]
    log.info("Deleting documents")
    for element in list_ids:
        collection.delete_one({'_id': element})
    """requests = [DeleteMany({'_id': {"$in": list_ids}})]
    try:
        collection.bulk_write(requests)
    except BulkWriteError as bwe:
        log.info(bwe.details)"""


    time.sleep(1)
    end = time.time()
    log.info(f"Process of deleting duplicates data took {end - start} seconds")

    """pipeline=[{"$group": {
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
                     }]"""


    """requests = []
        for document in collection.aggregate(pipeline, allowDiskUse=True):
            it = iter(document["uniqueIds"])
            next(it)
            for id in it:
                requests.append(DeleteOne({"_id": id}))
        collection.bulk_write(requests)"""
    #regions_range = [x for x in range(1, 380)]





    #collection.delete_many({"_id": {"$in": [ids_to_drop]}})



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
    for i in range(1, 381):
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
    #get_nodes_from_way("localhost", 27017)
    remove_duplicates()
    #connect("localhost", 27017, "regions")
    #delete_elements_col("localhost", 27017, "regions")

