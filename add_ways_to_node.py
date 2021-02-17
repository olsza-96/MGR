import ssl

from pymongo import MongoClient
from pymongo.errors import OperationFailure
import logging as log
import time

log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")


def get_nodes_from_way(start: int, stop:int):
    log.info(f"Connecting to the database")
    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    db = connection.Poland_spatial_data
    attributes = {"nodes": 1, "landuse": 1, "id":1, "_id": 0}
    for i in range(start, stop):
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

if __name__ == "__main__":
    get_nodes_from_way(start=365, stop=381)
