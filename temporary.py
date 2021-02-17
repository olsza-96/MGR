import ssl

from pymongo import MongoClient, DeleteMany, UpdateOne
from pymongo.errors import OperationFailure, BulkWriteError
import logging as log
import time
import pathlib as p
import os
import json
from bson import objectid, BSON

log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")


def delete_duplicates(collection: str):

    start = time.time()
    log.info(f"Connecting to the database")
    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

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

def delete_elements_col(collection: str, list_remove: list):
    start = time.time()
    log.info(f"Connecting to the database")
    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    with connection:
        log.info(f"Connected to {collection}")
        db = connection.Poland_spatial_data
        current_collection = db[collection]

        current_collection.remove({"region_id": {"$in": list_remove}})
        #current_collection.remove({"region_id": 219, "landuse": {"$exists": "False"}})
        time.sleep(1)
        end = time.time()
        log.info(f"Process of inserting data took {end - start} seconds")


def get_nodes_from_way():
    log.info(f"Connecting to the database")
    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    db = connection.Poland_spatial_data
    cur = db["testing_col"].find({})
    print(f"No of documents in testing col: {len(list(cur))}")
    attributes = {"nodes": 1, "landuse": 1, "id":1, "_id": 0}
    for i in range(185, 190):
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
            cur = db["testing_col"].find_one({"way_id": element["id"]})

            if cur != None:
                log.info("Already in db")
                continue
            else:
                db["testing_col"].update_many({"id": {"$in": element["nodes"]}},
                                     {"$set": {"landuse": element["landuse"],
                                               "way_id": element["id"]}},
                                     upsert=False
                                      )


def update_from_file(file):
    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    with connection:
        log.info(f"Inserting data to collection")
        db = connection.Poland_spatial_data
        current_collection = db["testing_col"]

        for element in file:
            log.info(f"Node_id: {element['id']}")
            current_collection.update({"id": element["id"]},
                                      {"$set": {"landuse": element["landuse"],
                                                "way_id": element["way_id"]}},
                                      upsert=False
                                      )

def insert_to_collection(documents: list):
    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    db = connection.Poland_spatial_data
    collection = db["ways"]
    for document in documents:
        cursor_check = collection.find({"id": document["id"]})
        if cursor_check.count() == 0:
            log.info("Inserting document")
            collection.insert_one(document)
        else:
            log.info(f"The cursor for {document['id']} already exists")

        collection.insert(document)


def read_json_file(f_name: str):
    with open(f_name) as json_file:
        data = json.load(json_file)

    #region_ranges = [x for x in range(180,200)]
    #output_data = [x for x in data if (x["region_id"] in region_ranges)]

    output_data = [x for x in data if ("landuse" in x.keys())]
    output_data = [{k: v for k,v in d.items() if k!="_id"} for d in output_data]

    print(f"Length {len(output_data)}")

    #with open("filtered_nodes_180_200.json", "w") as json_out:
    #    json.dump(output_data, json_out)

    return output_data

def insert_to_db(file):
    with open(file) as json_file:
        data = json.load(json_file)

    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    with connection:
        log.info(f"Connected to collection")
        db = connection.Poland_spatial_data
        current_collection = db["regions"]
        for i in range(1, 381):
            log.info(f"Inserting data for region {i}")
            data_region = [x for x in data if (x["id"] ==i)]
            data_region = [{k: v for k,v in d.items() if k!="_id"} for d in data_region]
            if len(data_region) != 0:
                current_collection.insert_many(data_region, ordered=False)
            else:
                continue
        #save_file(data_region, i)
        #data_region = [{k: v for k,v in d.items() if k!= "_id"} for d in data_region]

    #insert_to_collection(data_region)
    #output_data = [x for x in data if (x["region_id"] in region_ranges)]

def save_file(data:list, i:int):
    folder_path: p.Path = p.Path.cwd().joinpath("json_files")
    file_path: p.Path = folder_path.joinpath(f"input_nodes_region_{i}_{i+9}.json")
    with file_path.open(mode="w", encoding="utf-8") as written_file:
        log.info(f"Saving file input_nodes_region_{i}_{i+9}.json")
        json.dump(data, written_file)

def remove_duplicates():
    with open("/Users/Olga/PycharmProjects/MGR_Project/MGR/duplicates.txt", "r") as read_file:
        file = read_file.readlines()
    file = file[0].replace("[", "").replace("]", "")
    file_list = file.split(",")
    file_list = [x.strip(" ").strip('"') for x in file_list]

    #encoded = BSON.encode(file)
    list_ids = [objectid.ObjectId(x) for x in file_list]

    log.info(len(list_ids))

    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    db = connection.Poland_spatial_data
    collection = db["testing_col"]
    log.info("Deleting documents")
    for x in range(0,len(list_ids), 10000):
        log.info(f"{x} {x+10000}")
        to_delete = list_ids[x:x+10000]
        requests = [DeleteMany({"_id": {"$in": to_delete}})]
        try:
            collection.bulk_write(requests)
        except BulkWriteError as bwe:
            log.info(bwe.details)


def bulk_update_collection(file: list):
    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    db = connection.Poland_spatial_data
    collection = db["testing_col"]
    log.info("Updating documents")
    for x in range(0,len(file), 10000):
        log.info(f"{x} {x+10000}")
        to_update = file[x:x+10000]
        requests = [UpdateOne({"id": to_update[i]["id"]},
                              {"$set": {"landuse": to_update[i]["landuse"], "way_id": to_update[i]["way_id"]}})
                    for i in range(len(to_update))]
        try:
            log.info(file[x]["id"])
            collection.bulk_write(requests)
        except BulkWriteError as bwe:
            log.info(bwe.details)

if __name__ == "__main__":
    #list_regions = [150, 175, 181, 182, 183, 184, 185, 186]
    #get_nodes_from_way()

    #delete_elements_col("ways",list_regions)
    #out_nodes = read_json_file("json_files/output_nodes_200_250(no_219).json")
    #update_from_file(out_nodes)
    #insert_to_db("json_files/regions_backup.json")
    #delete_duplicates("ways")
    #remove_duplicates()
    file = read_json_file("/Users/Olga/PycharmProjects/MGR_Project/MGR/json_files/out_small_coll.json")
    bulk_update_collection(file)
