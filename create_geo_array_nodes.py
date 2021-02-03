from pymongo import MongoClient
from pymongo.errors import OperationFailure
import math
import logging as log
import time


log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")

def connect(host: str, port: int):
    log.info(f"Connecting to the database")
    connection = MongoClient(host, port)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    db = connection.Poland_spatial_data
    current_collection = db["nodes"]

    for i in range(6,381):
        start = time.time()
        log.info(f"Getting data for region {i}")
        region_nodes = iterate_over_region(i, current_collection)
        if region_nodes != 0:
            send_to_db(region_nodes, db["testing_col"])
        time.sleep(1)
        end = time.time()
        log.info(f"Process took {(end - start) / 60} minutes")

def iterate_over_region(region_id: int, collection):
    final_nodes_list = []
    cursor = collection.find({"region_id": region_id})
    data_nodes = list(cursor)
    if len(data_nodes)!=0:
        for element in data_nodes:
            element["coordinates"] = [element["lon"], element["lat"]]
            element.pop("lon")
            element.pop("lat")
            final_nodes_list.append(element)

        return final_nodes_list
    else:
        return 0

def send_to_db(data_to_send: list, collection):

    collection.insert_many(data_to_send)


if __name__ == "__main__":
    connect('localhost', 27017)